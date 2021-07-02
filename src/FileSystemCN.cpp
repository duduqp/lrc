//
// Created by 杜清鹏 on 2021/4/1.
//

#include <spdlog/sinks/basic_file_sink.h>
#include "ToolBox.h"
#include "FileSystemCN.h"
#include "coordinator.grpc.pb.h"
#include "combination_generator.h"

namespace lrc {
    FileSystemCN::FileSystemCN(const std::string &mConfPath,
                               const std::string &mMetaPath,
                               const std::string &mLogPath,
                               const std::string &mClusterPath) : m_fsimpl(mConfPath, mMetaPath, mLogPath,
                                                                           mClusterPath) {
        m_cn_logger = m_fsimpl.getMCnLogger(); //must wait until impl initialized first!
        if (!m_fsimpl.isMInitialized()) {
            m_cn_logger->error("initialize FileSystemImpl failed!");
            return;
        }
        m_initialized = true;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::createDir(::grpc::ServerContext *context, const ::coordinator::Path *request,
                                            ::coordinator::RequestResult *response) {
        auto _dstpath = request->dstpath();
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::uploadStripe(::grpc::ServerContext *context, const ::coordinator::StripeInfo *request,
                                               ::coordinator::StripeDetail *response) {
        std::unique_lock ulk1(m_stripeupdatingcount_mtx);
        std::unique_lock ulk2(m_fsimage_mtx);
        int stripeid = m_fs_nextstripeid++;
        auto retstripeloc = response->mutable_stripelocation();
        int k = request->stripe_k();
        int l = request->stripe_l();
        int g = request->stripe_g();
        auto[cand_dn, cand_lp, cand_gp] = placement_resolve(
                {k, l, g, 1}, m_placementpolicy);
        if (m_fs_image.count(stripeid)) {
            goto addstripelocation;
        } else {
            if (stripe_in_updating.count(stripeid)) {
                return grpc::Status::CANCELLED;
            }

            StripeLayoutGroup combined;
            for (const auto &dn : cand_dn) {
                combined.insert(dn);
            }
            for (const auto &lp : cand_lp) {
                combined.insert(lp);
            }
            for (const auto &gp : cand_gp) {
                combined.insert(gp);
            }
            stripe_in_updating.insert({stripeid, combined});
            stripe_in_updatingcounter[stripeid] = stripe_in_updating[stripeid].size();
        }

        m_cn_logger->info("picked datanodes for storing stripe {} :", stripeid);
        for (auto c:stripe_in_updating[stripeid]) {
            m_cn_logger->info("{}", c.first);
        }
        addstripelocation:
        std::vector<std::string> dn_uris(k);
        std::vector<std::string> lp_uris(l);
        std::vector<std::string> gp_uris(g);
        for (const auto &p:cand_dn) {
            const auto &[_uri, _stripegrp] = p;
            const auto &[_blkid, _type, _flag] = _stripegrp;
            dn_uris[_blkid] = _uri;
            if (!askDNhandling(p.first, stripeid)) return grpc::Status::CANCELLED;
        }


        for (const auto &p:cand_lp) {
            const auto &[_uri, _stripegrp] = p;
            const auto &[_blkid, _type, _flag] = _stripegrp;
            lp_uris[_blkid] = _uri;
            if (!askDNhandling(p.first, stripeid)) return grpc::Status::CANCELLED;
        }
        for (const auto &p:cand_gp) {
            const auto &[_uri, _stripegrp] = p;
            const auto &[_blkid, _type, _flag] = _stripegrp;
            gp_uris[_blkid] = _uri;
            if (!askDNhandling(p.first, stripeid)) return grpc::Status::CANCELLED;
        }


        for (const auto &uri : dn_uris) {
            retstripeloc->add_dataloc(uri);
        }
        for (const auto &uri : lp_uris) {
            retstripeloc->add_localparityloc(uri);
        }
        for (const auto &uri : gp_uris) {
            retstripeloc->add_globalparityloc(uri);
        }
        auto stripeId = response->mutable_stripeid();
        stripeId->set_stripeid(stripeid);
        return grpc::Status::OK;
    }

    FileSystemCN::FileSystemImpl::FileSystemImpl(const std::string &mConfPath, const std::string &mMetaPath,
                                                 const std::string &mLogPath,
                                                 const std::string &mClusterPath)
            : m_conf_path(mConfPath), m_meta_path(mMetaPath), m_log_path(mLogPath), m_cluster_path(mClusterPath) {
        m_cn_logger = spdlog::basic_logger_mt("cn_logger", mLogPath, true);
        if (!std::filesystem::exists(std::filesystem::path{m_conf_path})) {
            m_cn_logger->error("configure file not exist!");
            return;
        }
        auto init_res = initialize();
        if (!init_res) {
            m_cn_logger->error("configuration file error!");
            return;
        }

        auto cluster_res = initcluster();
        if (!cluster_res) {
            m_cn_logger->error("cluster file error!");
            return;
        }

        auto m_cluster_info_backup = m_cluster_info;
        /* init all stubs to dn */
        for (int i = 0; i < m_cluster_info_backup.size(); ++i) {

            auto dn_alive = std::vector<std::string>();
            for (auto p:m_cluster_info_backup[i].datanodesuri) {
                //filter out offline DNs
                //by call stubs checkalive
                auto _stub = datanode::FromCoodinator::NewStub(
                        grpc::CreateChannel(p, grpc::InsecureChannelCredentials()));

                int retry = 3;//default redetect 3 times
                while (0 != retry) {

                    grpc::ClientContext clientContext;
                    datanode::CheckaliveCMD Cmd;
                    datanode::RequestResult result;
                    grpc::Status status;

                    status = _stub->checkalive(&clientContext, Cmd, &result);


                    if (status.ok()) {
                        m_cn_logger->info("{} is living !", p);
                        if (!result.trueorfalse()) {
                            m_cn_logger->warn("but not initialized!");
                            // 3 * 10s is deadline
                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                            retry--;
                        } else {
                            dn_alive.push_back(p);
                            m_dn_ptrs.insert(std::make_pair(p, std::move(_stub)));
                            retry = 0;
                        }
                    } else {
                        m_dn_info.erase(p);
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        retry--;
                    }

                }
            }
            if (!dn_alive.empty()) m_cluster_info[i].datanodesuri = dn_alive;
            else {
                //whole cluster offline
                m_cluster_info.erase(i);
            }
            //timeout,this node is unreachable ...
        }

        if (!std::filesystem::exists(std::filesystem::path{mMetaPath})) {
            auto clear_res = clearexistedstripes();
            if (!clear_res) {
                m_cn_logger->error("Metapath {} does not exists , clear file system failed!", mMetaPath);
                return;
            }
            //create mMetaPath
            std::filesystem::create_directory(std::filesystem::path{m_meta_path}.parent_path());

        } else {
//            loadhistory(); //todo
            clearexistedstripes();
        }
        m_cn_logger->info("cn initialize success!");
        m_initialized = true;
    }

    bool FileSystemCN::FileSystemImpl::initialize() {
        /* set default ecschema initialize all DN stubs , load DN info Cluster info */
        //parse /conf/configuration.xml
        //parse xml
        pugi::xml_document xdoc;
        xdoc.load_file(m_conf_path.c_str(), pugi::parse_default, pugi::encoding_utf8);
        auto propertynode = xdoc.child("properties").child("property");
        for (auto propattr = propertynode.first_attribute(); propattr; propattr = propattr.next_attribute()) {
            auto propname = propattr.name();
            auto propvalue = propattr.value();
            if (std::string{"fs_uri"} == propname) {
                m_fs_uri = propvalue;
                std::cout << "my coordinator uri :" << propvalue << std::endl;
            }
        }

        return true;
    }

    bool FileSystemCN::FileSystemImpl::initcluster() {
        //parse  cluster.xml
        try {
            pugi::xml_document xdoc;
            xdoc.load_file(m_cluster_path.c_str());
            auto clustersnode = xdoc.child("clusters");
            for (auto clusternode = clustersnode.child(
                    "cluster"); clusternode; clusternode = clusternode.next_sibling()) {

                auto id = clusternode.attribute("id").value();
                auto gatewayuri = clusternode.attribute("gateway").value();
                auto datanodes = clusternode.child("nodes");
                int cluster_id = std::stoi(id);
                DataNodeInfo dninfo;
                dninfo.clusterid = cluster_id;
                dninfo.clusterid = cluster_id;
                dninfo.crosscluster_routeruri = gatewayuri;

                std::vector<std::string> dns;
                for (auto eachdn = datanodes.first_child(); eachdn; eachdn = eachdn.next_sibling()) {
                    m_dn_info[eachdn.attribute("uri").value()] = dninfo;
                    dns.push_back(eachdn.attribute("uri").value());
                }
                ClusterInfo clusterInfo{dns, gatewayuri, cluster_id, 0};
                m_cluster_info[cluster_id] = clusterInfo;

            }
        } catch (...) {
            return false;
        }
        return true;
    }

    bool FileSystemCN::FileSystemImpl::isMInitialized() const {
        return m_initialized;
    }

    const std::shared_ptr<spdlog::logger> &FileSystemCN::FileSystemImpl::getMCnLogger() const {
        return m_cn_logger;
    }

    const std::string &FileSystemCN::FileSystemImpl::getMFsUri() const {
        return m_fs_uri;
    }

    const ECSchema &FileSystemCN::FileSystemImpl::getMFsDefaultecschema() const {
        return m_fs_defaultecschema;
    }

    const std::unordered_map<int, std::vector<std::string>> &FileSystemCN::FileSystemImpl::getMFsImage() const {
        return m_fs_image;
    }

    const std::unordered_map<std::string, DataNodeInfo> &FileSystemCN::FileSystemImpl::getMDnInfo() const {
        return m_dn_info;
    }

    bool FileSystemCN::FileSystemImpl::clearexistedstripes() {
        //call stubs to invoke DNs clear operation
        datanode::ClearallstripeCMD clearallstripeCmd;
        grpc::Status status;
        for (auto &stub:m_dn_ptrs) {
            grpc::ClientContext clientContext;
            datanode::RequestResult result;
            status = stub.second->clearallstripe(&clientContext, clearallstripeCmd, &result);
            if (status.ok()) {
                if (!result.trueorfalse()) {
                    m_cn_logger->error("{} clear all stripe failed!", stub.first);
                    return false;
                }
            } else {
                m_cn_logger->error("{} rpc error!", stub.first);
            }
        }
        return true;
    }

    void FileSystemCN::FileSystemImpl::loadhistory() {
        //parse /meta/fsimage.xml
        pugi::xml_document xdoc;
        xdoc.load_file("./meta/fsimage.xml");

        auto histnode = xdoc.child("history");
        if (!histnode) return;
        auto schemanode = histnode.child("ecschema");
        auto schemastr = std::string(schemanode.value());
        //k,l,g
        int k, l, g;
        int pos1 = 0, pos2 = 0;
        pos2 = schemastr.find(',', pos1);
        k = std::stoi(schemastr.substr(pos1, pos2 - pos1));
        pos1 = pos2;
        pos2 = schemastr.find(',', pos1);
        l = std::stoi(schemastr.substr(pos1, pos2));
        g = std::stoi(schemastr.substr(pos2));

        auto stripesnode = histnode.child("stripes");
        std::stringstream uri;
        for (auto stripenode = stripesnode.child("stripe"); stripenode; stripenode = stripenode.next_sibling()) {
            std::vector<std::string> _dns;
            int id = std::stoi(stripenode.attribute("id").value());
            std::string urilist = std::string(stripenode.attribute("nodes").value());
            int cursor = 0;
            while (cursor < urilist.size()) {
                if (urilist[cursor] != '#') {
                    uri.putback(urilist[cursor]);
                } else {
                    _dns.push_back(uri.str());
                    uri.clear();
                }
                cursor++;
            }
            m_fs_image.insert({id, _dns});
        }
    }

    void FileSystemCN::FileSystemImpl::updatestripeupdatingcounter(int stripeid, std::string fromdatanode_uri) {

        std::scoped_lock lockGuard(m_stripeupdatingcount_mtx);
        std::cout << fromdatanode_uri << " updatestripeupdatingcounter for" << stripeid << std::endl;

        // stripe_in_updating[stripeid][fromdatanode_uri] = true; //this line truely distributed env only
        stripe_in_updatingcounter[stripeid]--;
        std::cout << this << " : " << stripe_in_updatingcounter[stripeid] << std::endl;
        if (stripe_in_updatingcounter[stripeid] == 0)//all blks are ready in DN
        {
            if (!m_fs_image.contains(stripeid)) {
                m_fs_image.insert({stripeid, std::vector<std::string>()});

                std::vector<std::string> datauris;
                std::vector<std::string> lpuris;
                std::vector<std::string> gpuris;
                for (auto &p :stripe_in_updating[stripeid]) {
                    const auto &[_stripeuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _flag] = _stripegrp;
                    if (TYPE::DATA == _type) {
                        datauris.push_back(_stripeuri);
                    } else if (TYPE::LP == _type) {
                        lpuris.push_back(_stripeuri);
                    } else {
                        gpuris.push_back(_stripeuri);
                    }
                    m_dn_info[_stripeuri].stored_stripeid.insert(stripeid);
                }
                m_fs_image[stripeid].insert(
                        m_fs_image[stripeid].end(), datauris.begin(), datauris.end());
                m_fs_image[stripeid].push_back("d");//marker
                m_fs_image[stripeid].insert(
                        m_fs_image[stripeid].end(), lpuris.begin(), lpuris.end());
                m_fs_image[stripeid].push_back("l");//marker
                m_fs_image[stripeid].insert(
                        m_fs_image[stripeid].end(), gpuris.begin(), gpuris.end());
                m_fs_image[stripeid].push_back("g");//marker

                //associate schema to this stripe
//                ECSchema ecSchema(datauris.size(), lpuris.size(), gpuris.size(), m_fs_defaultecschema.blksize);
//                m_fs_stripeschema[ecSchema].insert(stripeid);
            }
            stripe_in_updating.erase(stripeid);
            stripe_in_updatingcounter.erase(stripeid);
            m_updatingcond.notify_all();
        }
    }

    void FileSystemCN::FileSystemImpl::flushhistory() {

        std::scoped_lock scopedLock(m_fsimage_mtx);
        //flush back fs_image
        pugi::xml_document xdoc;
        auto rootnode = xdoc.append_child("history");
        auto ecschemanode = rootnode.append_child("ecschema");
        std::string ec{std::to_string(m_fs_defaultecschema.datablk)
                       + std::to_string(m_fs_defaultecschema.localparityblk)
                       + std::to_string(m_fs_defaultecschema.globalparityblk)};
        ecschemanode.set_value(ec.c_str());
        auto stripesnode = rootnode.append_child("stripes");
        std::string urilist;
        for (auto p:m_fs_image) {
            auto singlestripenode = stripesnode.append_child("stripe");
            singlestripenode.append_child("id").set_value(std::to_string(p.first).c_str());
            auto urilistsnode = singlestripenode.append_child("nodes");
            for (auto &nodeuri : p.second) {
                //dn1#dn2#...#*#lp1#lp2#...#*#gp1#gp2#...#*#
                urilist.append(("d" == nodeuri || "l" == nodeuri || "g" == nodeuri) ? "*" : nodeuri).append("#");
            }
            //throw away last"#"
            urilist.pop_back();
            singlestripenode.set_value(urilist.c_str());
            urilist.clear();
        }
        xdoc.save_file("./meta/fsimage.xml");
    }

    FileSystemCN::FileSystemImpl::~FileSystemImpl() {
        m_cn_logger->info("cn im-memory image flush back to metapath!");
        flushhistory();
    }


    std::vector<std::tuple<int, int, int>>
    FileSystemCN::FileSystemImpl::singlestriperesolve(const std::tuple<int, int, int> &para) {
        std::vector<std::tuple<int, int, int>> ret;
        auto[k, l, g] = para;
        int r = k / l;

        //cases
        //case1:
        int theta1 = 0;
        int theta2 = 0;

        if (r <= g) {
            theta1 = g / r;
            int i = 0;
            for (i = 0; i + theta1 <= l; i += theta1) ret.emplace_back(theta1 * r, theta1, 0);
            if (i < l) {
                ret.emplace_back((l - i) * r, (l - i), 0);
            }
            ret.emplace_back(0, 0, g);
            ret.emplace_back(-1, theta1, l - i);
        } else if (0 == (r % (g + 1))) {
            theta2 = r / (g + 1);
            for (int i = 0; i < l; ++i) {
                for (int j = 0; j < theta2; ++j) ret.emplace_back(g + 1, 0, 0);
            }
            ret.emplace_back(0, l, g);
            ret.emplace_back(-1, -1, 0);
        } else {
            int m = r % (g + 1);
            theta2 = r / (g + 1);
            //each local group remains m data blocks and 1 lp block
            // m[<=>r] < g -> case1
            theta1 = g / m;
            for (int i = 0; i < l; ++i) {
                for (int j = 0; j < theta2; ++j) {
                    ret.emplace_back(g + 1, 0, 0);
                }
            }

            ret.emplace_back(-1, -1, -1);//acts as a marker

            int i = 0;
            for (i = 0; i + theta1 <= l; i += theta1) ret.emplace_back(theta1 * m, theta1, 0);
            if (i < l) {
                ret.emplace_back((l - i) * m, (l - i), 0);
            }
            ret.emplace_back(0, 0, g);

            ret.emplace_back(-1, theta1, l - i);
        }
        return ret;
    }

    std::vector<std::tuple<std::vector<int>, std::vector<int>, std::vector<int>>>
    FileSystemCN::FileSystemImpl::generatelayout(const std::tuple<int, int, int> &para,
                                                 FileSystemCN::FileSystemImpl::PLACE placement, int stripenum,
                                                 int step) {

        int c = m_cluster_info.size();
        std::vector<int> totalcluster(c, 0);
        std::iota(totalcluster.begin(), totalcluster.end(), 0);
        std::vector<std::tuple<std::vector<int>, std::vector<int>, std::vector<int>>> stripeslayout;
        auto[k, l, g] = para;
        int r = k / l;

        auto layout = singlestriperesolve(para);
        if (placement == PLACE::SPARSE) {
            for (int j = 0; j * step < stripenum; ++j) {
                std::vector<std::vector<int>> datablock_location(step, std::vector<int>(k, -1));
                std::vector<std::vector<int>> lpblock_location(step, std::vector<int>(l, -1));
                std::vector<std::vector<int>> gpblock_location(step, std::vector<int>(g, -1));
                if (r <= g) {
                    // case1
                    // s1: D0D1L0 D2D3L1 G0G1G2
                    // s2: D0D1L0 D2D3L1 G0G1G2
                    std::vector<int> clusters(totalcluster.begin(), totalcluster.end());
                    std::random_shuffle(clusters.begin(), clusters.end());
                    int idx_l = 0;
                    int idx_d = 0;
                    int n = 0;
                    //to pack residue
                    auto[_ignore1, theta1, res_grpnum] = layout.back();

                    int lim = (0 == res_grpnum ? layout.size() - 2 : layout.size() - 3);
                    for (int i = 0; i < lim; ++i) {
                        auto[cluster_k, cluster_l, cluster_g]=layout[i];
                        for (int u = 0; u < step; ++u) {
                            int idx_l1 = idx_l;
                            int idx_d1 = idx_d;
                            for (int x = 0; x < cluster_l; ++x) {
                                lpblock_location[u][idx_l1] = clusters[n];
                                idx_l1++;
                                for (int m = 0; m < r; ++m) {
                                    datablock_location[u][idx_d1] = clusters[n];
                                    idx_d1++;
                                }
                            }
                            n++;
                        }
                        idx_d += cluster_k;
                        idx_l += cluster_l;
                    }
                    if (res_grpnum) {
                        int cur_res_grp = 0;
                        for (int x = 0; x < step; ++x) {
                            if (cur_res_grp + res_grpnum > theta1) {
                                n++;//next cluster
                                cur_res_grp = 0;//zero
                            }

                            int cur_res_idxl = idx_l;
                            int cur_res_idxd = idx_d;

                            for (int y = 0; y < res_grpnum; ++y) {
                                lpblock_location[x][cur_res_idxl++] = clusters[n];
                                for (int i = 0; i < r; ++i) {
                                    datablock_location[x][cur_res_idxd++] = clusters[n];
                                }
                            }
                            cur_res_grp += res_grpnum;
                            //put res_grpnum residue group into cluster
                        }
                    }
                    //global cluster
                    for (int x = 0; x < step; ++x) {
                        for (int i = 0; i < g; ++i) gpblock_location[x][i] = clusters[n];
                        n++;
                    }
                } else if (0 == r % (g + 1)) {
                    //case2
                    //D0D1D2   D3D4D5   G0G1L0L1
                    std::vector<int> clusters(totalcluster.begin(), totalcluster.end());
                    std::random_shuffle(clusters.begin(), clusters.end());
                    int n = 0;
                    for (int i = 0; i < step; ++i) {
                        int idxd = 0;
                        for (int x = 0; x < layout.size() - 2; ++x) {
                            for (int y = 0; y < g + 1; ++y) {
                                datablock_location[i][idxd++] = clusters[n];
                            }
                            n++;
                        }
                        //g and l parity cluster
                        for (int x = 0; x < g; ++x) {
                            gpblock_location[i][x] = clusters[n];
                        }
                        for (int x = 0; x < l; ++x) {
                            lpblock_location[i][x] = clusters[n];
                        }
                        n++;
                    }
                } else {
                    //special case3
                    std::vector<int> clusters(totalcluster.begin(), totalcluster.end());
                    std::random_shuffle(clusters.begin(), clusters.end());
                    int residue = std::find(layout.cbegin(), layout.cend(), std::tuple<int, int, int>{-1, -1, -1}) -
                                  layout.cbegin();
                    int round = (r / (g + 1)) * (g + 1);
                    // [round,r)*cursor+lp
                    auto[_ignore1, pack_cluster_cap, frac_cluster_num] = layout.back();
                    int s = 0;
                    int packed_cluster_num = 0;
                    int packed_residue = 0;
                    if (frac_cluster_num) {
                        packed_cluster_num = (pack_cluster_cap / frac_cluster_num);
                        packed_residue = step % packed_cluster_num;
                        s = (0 != packed_residue) ? step / packed_cluster_num + 1 : step / packed_cluster_num;
                    }//require s clusters to pack the residue groups
                    int n = s;
                    int m = residue + 1;
                    int x = 0;
                    int y = 0;
                    int cursor = 1;
                    int lim = (0 == frac_cluster_num ? layout.size() - 2 : layout.size() - 3);

                    //TODO : cache optimization nested for loop
                    for (; m < lim; ++m) {
                        auto[residuecluster_k, residuecluster_l, residuecluster_g] = layout[m];
                        int residuecluster_r = residuecluster_k / residuecluster_l;
                        for (int u = 0; u < step; ++u) {
                            int cur_cursor = cursor;
                            for (x = 0; x < residuecluster_l; ++x) {
                                for (y = 0; y < residuecluster_r; ++y) {
                                    datablock_location[u][(cur_cursor - 1) * r + y + round] = clusters[n];
                                }
                                lpblock_location[u][cur_cursor - 1] = clusters[n];
                                cur_cursor++;
                            }
                            n++;
                        }
                        cursor += residuecluster_l;

                    }
                    if (frac_cluster_num) {
                        auto[res_k, res_l, res_g] = layout[m];
                        int res_r = res_k / res_l;
                        int cur_back = cursor;
                        for (int u1 = 0; u1 < step; ++u1) {
                            cursor = cur_back;
                            if (0 != u1 && 0 == (u1 % packed_cluster_num)) {
                                n++;
                            }
                            for (int x1 = 0; x1 < res_l; ++x1) {
                                lpblock_location[u1][cursor - 1] = clusters[n];
                                for (int y1 = 0; y1 < res_r; ++y1) {
                                    datablock_location[u1][(cursor - 1) * r + round + y1] = clusters[n];
                                }
                                cursor++;
                            }
                        }
                    }

                    n++;
                    //pack remained

                    for (int u = 0; u < step; ++u) {
                        for (x = 0; x < l; ++x) {
                            for (y = 0; y < round; ++y) {
                                datablock_location[u][x * r + y] = clusters[n];
                                if (0 == ((y + 1) % (g + 1))) n++;
                            }
                        }
                    }
                    for (int u = 0; u < step; ++u) {
                        for (x = 0; x < g; ++x) {
                            gpblock_location[u][x] = clusters[n];
                        }
                        n++;
                    }

                }
                for (int i = 0; i < step; ++i) {
                    stripeslayout.emplace_back(datablock_location[i], lpblock_location[i], gpblock_location[i]);
                }

                datablock_location.assign(step, std::vector<int>(k, -1));
                lpblock_location.assign(step, std::vector<int>(l, -1));
                gpblock_location.assign(step, std::vector<int>(g, -1));
            }
        } else if (placement == PLACE::COMPACT) {
            for (int j = 0; j * step < stripenum; ++j) {
                std::vector<int> clusters(totalcluster.begin(), totalcluster.end());
                std::random_shuffle(clusters.begin(), clusters.end());
                std::vector<int> datablock_location(k, -1);
                std::vector<int> lpblock_location(l, -1);
                std::vector<int> gpblock_location(g, -1);
                //ignore tuple if any element <0
                int idxd = 0;
                int idxl = 0;
                int n = 0;
                int r = k / l;
                int idxr = 0;
                int res_idxl = 0;
                int round = (r / (g + 1)) * (g + 1);
                for (auto cluster:layout) {
                    if (auto[_k, _l, _g] = cluster;_k < 0 || _l < 0 || _g < 0) {
                        continue;
                    } else {
                        //put _k data blocks ,_l lp blocks and _g gp blocks into this cluster

                        if (r <= g) {
                            for (int i = 0; i < _l; ++i) {
                                lpblock_location[idxl++] = clusters[n];
                            }
                            for (int i = 0; i < _k; ++i) {
                                datablock_location[idxd++] = clusters[n];
                            }
                            for (int i = 0; i < _g; ++i) {
                                gpblock_location[i] = clusters[n];
                            }

                            n++;
                        } else if (0 == (r % (g + 1))) {
                            //inner group index idxr
                            //group index idxl
                            //each cluster contain idxl group's subgroup
                            //D0D1   D2D3    G0L0
                            //D4D5   D6D7    G1L1

                            for (int i = 0; i < _k; ++i) {
                                datablock_location[idxl * r + idxr] = clusters[n];
                                idxr++;
                            }
                            if (idxr == r) {
                                idxl++;//+= theta1
                                idxr = 0;
                            }
                            for (int i = 0; i < _l; ++i) {
                                lpblock_location[i] = clusters[n];
                            }
                            for (int i = 0; i < _g; ++i) {
                                gpblock_location[i] = clusters[n];
                            }
                            n++;
                        } else {
                            //special case3
                            //D0D1D2   D3D4D5   D6L0   G1G2   D7D8D9   D10D11D12   D14D15D16   D17L2
                            //                  D13L1
                            if (0 == _l && 0 == _g) {
                                for (int i = 0; i < _k; ++i) {
                                    datablock_location[idxl * r + idxr] = clusters[n];
                                    idxr++;
                                }
                                if (idxr == round) {
                                    idxl++;
                                    idxr = 0;
                                }
                            } else if (0 == _g) {
                                idxr = round;
                                int res_idxr = idxr;
                                for (int i = 0; i < _l; ++i) {
                                    for (int x = 0; x < (_k / _l); ++x) {
                                        datablock_location[res_idxl * r + res_idxr] = clusters[n];
                                        res_idxr++;
                                        if (res_idxr == r) {
                                            lpblock_location[res_idxl] = clusters[n];
                                            res_idxl++;
                                            res_idxr = round;
                                        }
                                    }
                                }
                            } else {
                                //gp cluster
                                for (int i = 0; i < _g; ++i) {
                                    gpblock_location[i] = clusters[n];
                                }
                            }
                            n++;
                        }
                    }
                }

                for (int i = 0; i < step; ++i) {
                    stripeslayout.emplace_back(datablock_location, lpblock_location, gpblock_location);
                }
            }
        } else {

            //random
            std::vector<int> clusters(totalcluster.begin(), totalcluster.end());
            for (int j = 0; j < stripenum; ++j) {
                std::random_shuffle(clusters.begin(), clusters.end());
                std::vector<int> datablock_location(k, -1);
                std::vector<int> lpblock_location(l, -1);
                std::vector<int> gpblock_location(g, -1);
                int n = 0;
                int idxl = 0;
                int idxd = 0;
                int idxr = 0;
                if (r <= g) {
                    for (auto cluster:layout) {
                        auto[_k, _l, _g]=cluster;
                        if (_k < 0) continue;
                        for (int x = 0; x < _l; ++x) {
                            int _r = _k / _l;//_r == r
                            for (int y = 0; y < _r; ++y) {
                                datablock_location[idxd] = clusters[n];
                                idxd++;
                            }
                            lpblock_location[idxl] = clusters[n];
                            idxl++;
                        }

                        for (int x = 0; x < _g; ++x) {
                            gpblock_location[x] = clusters[n];
                        }
                        n++;
                    }
                } else if (0 == (r % (g + 1))) {
                    for (auto cluster:layout) {
                        auto[_k, _l, _g]= cluster;
                        if (_k < 0 || _l < 0 || _g < 0) continue;
                        for (int i = 0; i < _k; ++i) {
                            datablock_location[idxd++] = clusters[n];
                        }
                        for (int i = 0; i < _l; ++i) {

                            lpblock_location[idxl++] = clusters[n];
                        }
                        for (int i = 0; i < _g; ++i) {
                            gpblock_location[i] = clusters[n];
                        }
                        n++;
                    }
                } else {
                    int round = ((k / l) / (g + 1)) * (g + 1);

                    for (auto cluster:layout) {
                        auto[_k, _l, _g]= cluster;
                        if (_k < 0 || _l < 0 || _g < 0) continue;
                        if (_l == 0 && _g == 0) {
                            //normal cluster
                            for (int x = 0; x < g + 1; ++x) {
                                datablock_location[idxd++] = clusters[n];
                            }
                            if (0 == (idxd % round)) {
                                idxd = (idxd / r + 1) * r;
                            }
                            n++;
                        } else if (_g == 0) {
                            //residue cluster
                            for (int x = 0; x < _l; ++x) {
                                lpblock_location[idxl] = clusters[n];
                                for (int y = 0; y < (_k / _l); ++y) {
                                    datablock_location[idxl * r + round + y] = clusters[n];
                                }
                                idxl++;
                            }
                            n++;
                        } else {
                            //gp cluster
                            for (int x = 0; x < _g; ++x) {
                                gpblock_location[x] = clusters[n];
                            }
                        }

                    }

                }
                stripeslayout.emplace_back(datablock_location, lpblock_location, gpblock_location);
            }
        }
        return stripeslayout;
    }

    std::vector<FileSystemCN::FileSystemImpl::SingleStripeLayout>
    FileSystemCN::FileSystemImpl::layout_convert_helper(
            std::vector<SingleStripeLayout_bycluster> &layout, int step) {
        //{c0,c0,c0,c1,c1,c1...}{...}{cn,cn,cn,...} -> {d00 d01 d02,d10,d11,d12,...}
        std::vector<SingleStripeLayout> ret;
        for (int j = 0; j * step < layout.size(); ++j) {
            std::unordered_map<int, int> cluster_counter;
            std::unordered_set<int> done;
            for (int i = 0; i < step; ++i) {
                const auto &[data_cluster, lp_cluster, gp_cluster] = layout[j * step + i];
                for (auto c:data_cluster) {
                    if (cluster_counter.contains(c)) {
                        cluster_counter[c]++;
                    } else {
                        cluster_counter[c] = 1;
                    }
                }
                for (auto c:lp_cluster) {
                    if (cluster_counter.contains(c)) {
                        cluster_counter[c]++;
                    } else {
                        cluster_counter[c] = 1;
                    }
                }
                for (auto c:gp_cluster) {
                    if (cluster_counter.contains(c)) {
                        cluster_counter[c]++;
                    } else {
                        cluster_counter[c] = 1;
                    }
                }
            }
            std::unordered_map<int, std::vector<std::string>> picked_nodes;
            for (auto c:cluster_counter) {
                std::vector<std::string> picked;
                std::sample(m_cluster_info[c.first].datanodesuri.cbegin(), m_cluster_info[c.first].datanodesuri.cend(),
                            std::back_inserter(picked), c.second, std::mt19937{std::random_device{}()});
                picked_nodes.insert({c.first, picked});
            }
            //second pass
            for (int i = 0; i < step; ++i) {
                StripeLayoutGroup data_nodes;
                StripeLayoutGroup lp_nodes;
                StripeLayoutGroup gp_nodes;
                auto[data_cluster, lp_cluster, gp_cluster] = layout[j * step + i];

                int blk_id = 0;
                for (auto c:data_cluster) {
                    //block id
                    data_nodes.insert(
                            {picked_nodes[c][cluster_counter[c] - 1], std::make_tuple(blk_id, TYPE::DATA, false)});
                    blk_id++;
                    cluster_counter[c]--;
                    if (0 == cluster_counter[c]) {
                        cluster_counter.erase(c);
                    }
                }


                blk_id = 0;
                for (auto c:lp_cluster) {
                    lp_nodes.insert(
                            {picked_nodes[c][cluster_counter[c] - 1], std::make_tuple(blk_id, TYPE::LP, false)});
                    blk_id++;
                    cluster_counter[c]--;
                    if (0 == cluster_counter[c]) {
                        cluster_counter.erase(c);
                    }
                }

                blk_id = 0;
                for (auto c:gp_cluster) {
                    gp_nodes.insert(
                            {picked_nodes[c][cluster_counter[c] - 1], std::make_tuple(blk_id, TYPE::GP, false)});
                    blk_id++;
                    cluster_counter[c]--;
                    if (0 == cluster_counter[c]) {
                        cluster_counter.erase(c);
                    }
                }
                ret.emplace_back(data_nodes, lp_nodes, gp_nodes);
            }
        }
        return ret;
    }

    FileSystemCN::FileSystemImpl::SingleStripeLayout
    FileSystemCN::FileSystemImpl::placement_resolve(ECSchema ecSchema,
                                                    PLACE placement) {

        //propose step = 3 4 as future work
        //move correspoding cursor
        if (placement == PLACE::RANDOM) {
            if (random_placement_layout_cursor.contains(ecSchema)) {
                //if require resize
                int cursor = random_placement_layout_cursor[ecSchema];
                if (cursor == random_placement_layout[ecSchema].size()) {
                    //double size
                    int appendsize = cursor;
                    auto genentry = generatelayout(
                            {ecSchema.datablk, ecSchema.localparityblk, ecSchema.globalparityblk},
                            PLACE::RANDOM, appendsize);
                    auto appendentry = layout_convert_helper(genentry);
                    random_placement_layout[ecSchema].insert(random_placement_layout[ecSchema].cend(),
                                                             appendentry.cbegin(), appendentry.cend());
                }
                random_placement_layout_cursor[ecSchema]++;
                return random_placement_layout[ecSchema][cursor];
            } else {
                //initially 100 stripes
                //resize double ...
                auto initentry = generatelayout({ecSchema.datablk, ecSchema.localparityblk, ecSchema.globalparityblk},
                                                PLACE::RANDOM, 100);
                random_placement_layout.insert({ecSchema,
                                                layout_convert_helper(initentry)});
                random_placement_layout_cursor[ecSchema] = 0;
                return random_placement_layout[ecSchema][random_placement_layout_cursor[ecSchema]++];
            }
        } else if (placement == PLACE::COMPACT) {
            if (compact_placement_layout_cursor.contains(ecSchema)) {
                //if require resize
                int cursor = compact_placement_layout_cursor[ecSchema];
                if (cursor == compact_placement_layout[ecSchema].size()) {
                    //double size
                    int appendsize = cursor;
                    auto genentry = generatelayout(
                            {ecSchema.datablk, ecSchema.localparityblk, ecSchema.globalparityblk},
                            PLACE::COMPACT, appendsize);
                    auto appendentry = layout_convert_helper(genentry);
                    compact_placement_layout[ecSchema].insert(compact_placement_layout[ecSchema].cend(),
                                                              appendentry.cbegin(), appendentry.cend());
                }
                compact_placement_layout_cursor[ecSchema]++;
                return compact_placement_layout[ecSchema][cursor];
            } else {
                //initially 100 stripes
                //resize double ...
                auto initentry = generatelayout({ecSchema.datablk, ecSchema.localparityblk, ecSchema.globalparityblk},
                                                PLACE::COMPACT, 100);
                compact_placement_layout.insert({ecSchema,
                                                 layout_convert_helper(initentry)});
                compact_placement_layout_cursor[ecSchema] = 0;
                return compact_placement_layout[ecSchema][compact_placement_layout_cursor[ecSchema]++];
            }
        } else {
            if (sparse_placement_layout_cursor.contains(ecSchema)) {
                //if require resize
                int cursor = sparse_placement_layout_cursor[ecSchema];
                if (cursor == sparse_placement_layout[ecSchema].size()) {
                    //double size
                    int appendsize = cursor;
                    auto genentry = generatelayout(
                            {ecSchema.datablk, ecSchema.localparityblk, ecSchema.globalparityblk},
                            PLACE::SPARSE, appendsize);
                    auto appendentry = layout_convert_helper(genentry);
                    sparse_placement_layout[ecSchema].insert(sparse_placement_layout[ecSchema].cend(),
                                                             appendentry.cbegin(), appendentry.cend());
                }
                sparse_placement_layout_cursor[ecSchema]++;
                return sparse_placement_layout[ecSchema][cursor];
            } else {
                //initially 100 stripes
                //resize double ...
                auto initentry = generatelayout({ecSchema.datablk, ecSchema.localparityblk, ecSchema.globalparityblk},
                                                PLACE::SPARSE, 100);
                sparse_placement_layout.insert({ecSchema,
                                                layout_convert_helper(initentry)});
                sparse_placement_layout_cursor[ecSchema] = 0;
                return sparse_placement_layout[ecSchema][sparse_placement_layout_cursor[ecSchema]++];
            }
        }
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::uploadCheck(::grpc::ServerContext *context, const ::coordinator::StripeInfo *request,
                                              ::coordinator::RequestResult *response) {
        //handle client upload check
        //check if stripeid success or not
        std::unique_lock uniqueLock(m_stripeupdatingcount_mtx);
        //60s deadline
        auto res = m_updatingcond.wait_for(uniqueLock, std::chrono::seconds(60), [&]() {
            return !stripe_in_updatingcounter.contains(request->stripeid());
        });
        response->set_trueorfalse(res);
        if (res) flushhistory();
        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::reportblockupload(::grpc::ServerContext *context,
                                                    const ::coordinator::StripeId *request,
                                                    ::coordinator::RequestResult *response) {
        std::cout << "datanode " << context->peer() << " receive block of stripe " << request->stripeid()
                  << " from client successfully!\n";
        m_cn_logger->info("datanode {} receive block of stripe {} from client successfully!", context->peer(),
                          request->stripeid());
//        std::scoped_lock lockGuard(m_stripeupdatingcount_mtx);
        updatestripeupdatingcounter(request->stripeid(), context->peer());
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    bool FileSystemCN::FileSystemImpl::askDNhandling(const std::string &dnuri, int stripeid) {
        m_cn_logger->info("ask {} to wait for client uploading!", dnuri);

        grpc::ClientContext handlectx;
        datanode::RequestResult handlereqres;
        datanode::UploadCMD uploadCmd;
        auto status = m_dn_ptrs[dnuri]->handleupload(&handlectx, uploadCmd, &handlereqres);
        if (status.ok()) {
            return handlereqres.trueorfalse();
        } else {
            std::cout << "rpc askDNhandlestripe error!" << dnuri << std::endl;
            m_cn_logger->error("rpc askDNhandlestripe error!");
            return false;
        }
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::deleteStripe(::grpc::ServerContext *context, const ::coordinator::StripeId *request,
                                               ::coordinator::RequestResult *response) {

        std::cout << "delete stripe" << request->stripeid() << std::endl;
        std::scoped_lock slk(m_stripeupdatingcount_mtx, m_fsimage_mtx);
        for (auto dnuri:stripe_in_updating[request->stripeid()]) {
            grpc::ClientContext deletestripectx;
            datanode::StripeId stripeId;
            stripeId.set_stripeid(request->stripeid());
            datanode::RequestResult deleteres;
            m_dn_ptrs[dnuri.first]->clearstripe(&deletestripectx, stripeId, &deleteres);
            m_dn_info[dnuri.first].stored_stripeid.erase(request->stripeid());
        }
        //delete
        stripe_in_updating.erase(request->stripeid());
        stripe_in_updatingcounter.erase(request->stripeid());
        m_fs_image.erase(request->stripeid());

        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::listStripe(::grpc::ServerContext *context, const ::coordinator::StripeId *request,
                                             ::coordinator::StripeLocation *response) {

        std::scoped_lock scopedLock(m_fsimage_mtx);
        int stripeid = request->stripeid();

        auto &loc_str = m_fs_image[stripeid];
        int start = 0;
        for (int i = 0; i < 3; ++i) {
            while (start < loc_str.size() && "d" != loc_str[start]) {
                response->add_dataloc(loc_str[start]);
                start++;
            }
            start++;//skip "dlg"
            while (start < loc_str.size() && "l" != loc_str[start]) {
                response->add_localparityloc(loc_str[start]);
                start++;
            }
            start++;
            while (start < loc_str.size() && "g" != loc_str[start]) {
                response->add_globalparityloc(loc_str[start]);
                start++;
            }
        }


        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::downloadStripe(::grpc::ServerContext *context, const ::coordinator::StripeId *request,
                                                 ::coordinator::StripeDetail *response) {

        std::scoped_lock slk(m_fsimage_mtx);
        auto retstripeloc = response->mutable_stripelocation();
        int stripeid = request->stripeid();
        std::vector<std::string> datauris;//extractdatablklocation(request->stripeid());
        std::vector<std::string> lpuris;//extractlpblklocation(request->stripeid());
        std::vector<std::string> gpuris;//extractgpblklocation(request->stripeid());
        goto serverequest;
        if (m_fs_image.contains(stripeid)) {
            //this a  valid case
            //check alive first , if necessary ,perform lrc decode
            //lazy repair ? eager repair !
            //once detected a failure , decode whole stripe and re-deploy
            //todo : backgroud repair and serve request first if possible

            int start = 0;
            while (start < m_fs_image[stripeid].size() &&
                   m_fs_image[stripeid][start] != "d") {
                datauris.push_back(m_fs_image[stripeid][start]);
                start++;
            }
            auto alivedatanodes = checknodesalive(datauris);
            while (start < m_fs_image[stripeid].size() &&
                   m_fs_image[stripeid][start] != "l") {
                lpuris.push_back(m_fs_image[stripeid][start]);
                start++;
            }
            auto alivelpnodes = checknodesalive(lpuris);

            while (start < m_fs_image[stripeid].size() &&
                   m_fs_image[stripeid][start] != "g") {
                gpuris.push_back(m_fs_image[stripeid][start]);
                start++;
            }
            auto alivegpnodes = checknodesalive(datauris);

            //if it requires a repair

            if (std::find(alivedatanodes.begin(), alivedatanodes.end(), false) != alivedatanodes.end() &&
                std::find(alivelpnodes.begin(), alivelpnodes.end(), false) != alivelpnodes.end() &&
                std::find(alivegpnodes.begin(), alivegpnodes.end(), false) != alivegpnodes.end()) {
                //analysis
                bool decodable = analysisdecodable(datauris, alivedatanodes, lpuris, alivelpnodes, gpuris,
                                                   alivegpnodes);
                if (decodable) {
                    auto locallyrepairable = analysislocallyrepairable(datauris, alivedatanodes, lpuris, alivelpnodes);
                    if (locallyrepairable) {
                        bool res = dolocallyrepair(datauris, alivedatanodes, lpuris, alivelpnodes);
                    } else {
                        bool res = docompleterepair(datauris, alivedatanodes, lpuris, alivelpnodes, alivelpnodes,
                                                    alivegpnodes);
                    }
                    //refresh metadata
                } else {
                    m_cn_logger->warn("corrupted stripe {}", stripeid);
                    return grpc::Status::CANCELLED;
                }
            }
            //can serve download request
        } else {
            return grpc::Status::CANCELLED;
        }
        serverequest:
        int start = 0;
        while (start < m_fs_image[stripeid].size() &&
               m_fs_image[stripeid][start] != "d") {
            datauris.push_back(m_fs_image[stripeid][start]);
            start++;
        }
        //serve download
        bool res = askDNservepull(std::unordered_set<std::string>(datauris.cbegin(), datauris.cend()), "", stripeid);
        if (!res) {
            m_cn_logger->info("datanodes can not serve client download request!");
            return grpc::Status::CANCELLED;
        }

        for (int i = 0; i < datauris.size(); ++i) {
            retstripeloc->add_dataloc(datauris[i]);
        }

        std::cout << "returned locations!\n";
        return grpc::Status::OK;
    }

    bool FileSystemCN::FileSystemImpl::analysisdecodable(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                         std::vector<std::string> lp, std::vector<bool> alivelp,
                                                         std::vector<std::string> gp,
                                                         std::vector<bool> alivegp) {

        std::vector<int> remained(lp.size(), 0);
        int totalremain = 0;
        int r = gp.size();
        for (int i = 0; i < lp.size(); ++i) {
            //for each local group
            int currentgroupneedrepair = 0;
            for (int j = 0; j < r; ++j) {
                if (!alivedn[i * r + j]) currentgroupneedrepair++;
            }
            remained[i] = currentgroupneedrepair + (alivelp[i] ? -1 : 0);
            totalremain += remained[i];
        }

        if (totalremain <= r) {
            return true;
        } else {
            return false;
        }
    }

    bool
    FileSystemCN::FileSystemImpl::analysislocallyrepairable(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                            std::vector<std::string> lp, std::vector<bool> alivelp) {
        int r = dn.size() / lp.size();

        for (int i = 0; i < lp.size(); ++i) {
            //for each local group
            int currentgroupneedrepair = 0;
            for (int j = 0; j < r; ++j) {
                if (!alivedn[i * r + j]) currentgroupneedrepair++;
            }
            currentgroupneedrepair += (alivelp[i] ? -1 : 0);
            if (0 != currentgroupneedrepair) {
                return false;
            }
        }
        return true;
    }

    bool FileSystemCN::FileSystemImpl::dolocallyrepair(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                       std::vector<std::string> lp, std::vector<bool> alivelp) {

        int r = dn.size() / lp.size();
        for (int i = 0; i < lp.size(); ++i) {
            std::unordered_set<std::string> excludednodes(dn.begin() + i * r,
                                                          dn.begin() + (i + 1) * r);//this local group
            excludednodes.insert(lp[i]);
            for (int j = 0; j < r; ++j) {
                if (!alivedn[i * r + j]) {
                    excludednodes.erase(dn[i * r + j]);//exclude dead node
                    int clusterid = m_dn_info[dn[i * r + j]].clusterid;
                    //pick another dn
                    for (auto &cand:m_cluster_info[clusterid].datanodesuri) {
                        if (!excludednodes.contains(cand)) {
                            //
                            askDNservepull(excludednodes, uritoipaddr(cand), 0);
                            grpc::ClientContext localrepairctx;
                            datanode::NodesLocation stripeLocation;
                            datanode::RequestResult repairres;
                            auto status = m_dn_ptrs[cand]->dolocallyrepair(&localrepairctx, stripeLocation, &repairres);
                            if (status.ok()) {
                                return true;
                            } else {
                                //try another cand
                                ;
                            }
                        }

                    }
                    //fail
                    return false;
                }
            }
        }


    }

    bool FileSystemCN::FileSystemImpl::docompleterepair(std::vector<std::string> dn, std::vector<bool> alivedn,
                                                        std::vector<std::string> lp, std::vector<bool> alivelp,
                                                        std::vector<bool> gp, std::vector<bool> alivegp) {
        return false;
    }

    bool FileSystemCN::FileSystemImpl::askDNservepull(std::unordered_set<std::string> reqnodes, std::string src,
                                                      int stripeid) {
        for (const auto &node:reqnodes) {
            grpc::ClientContext downloadctx;
            datanode::DownloadCMD downloadCmd;
            datanode::RequestResult res;
            std::cout << "ask datanode : " << node << " to serve client download request" << std::endl;
            auto status = m_dn_ptrs[node]->handledownload(&downloadctx, downloadCmd, &res);
            if (!status.ok()) {
                std::cout << " datanode :" << node << " no response ! try next ... " << std::endl;
                //maybe have a blacklist
                m_cn_logger->info("choosen datanode {} , can not serve download request!", node);
                return false;
            }
        }
        return true;
    }

    grpc::Status FileSystemCN::FileSystemImpl::listAllStripes(::grpc::ServerContext *context,
                                                              const ::coordinator::ListAllStripeCMD *request,
                                                              ::grpc::ServerWriter<::coordinator::StripeLocation> *writer) {
        std::scoped_lock slk(m_fsimage_mtx);
        for (int i = 0; i < m_fs_image.size(); ++i) {
            coordinator::StripeLocation stripeLocation;
            int j = 0;
            for (; j < m_fs_image[i].size(); ++j) {
                if ("d" != m_fs_image[i][j]) {
                    stripeLocation.add_dataloc(m_fs_image[i][j]);
                    stripeLocation.add_dataloc(std::to_string(m_dn_info[m_fs_image[i][j]].clusterid));

                } else {
                    break;
                }
            }
            j++;
            for (; j < m_fs_image[i].size(); ++j) {
                if ("l" != m_fs_image[i][j]) {
                    stripeLocation.add_localparityloc(m_fs_image[i][j]);
                    stripeLocation.add_localparityloc(std::to_string(m_dn_info[m_fs_image[i][j]].clusterid));
                } else {
                    break;
                }
            }
            j++;
            for (; j < m_fs_image[i].size(); ++j) {
                if ("g" != m_fs_image[i][j]) {
                    stripeLocation.add_globalparityloc(m_fs_image[i][j]);
                    stripeLocation.add_globalparityloc(std::to_string(m_dn_info[m_fs_image[i][j]].clusterid));
                } else {
                    break;
                }
            }
            writer->Write(stripeLocation);
        }
        return grpc::Status::OK;
    }

    std::vector<bool> FileSystemCN::FileSystemImpl::checknodesalive(const std::vector<std::string> &vector) {
        return std::vector<bool>();
    }

    grpc::Status FileSystemCN::FileSystemImpl::downloadStripeWithHint(::grpc::ServerContext *context,
                                                                      const ::coordinator::StripeIdWithHint *request,
                                                                      ::coordinator::StripeLocation *response) {
        return Service::downloadStripeWithHint(context, request, response);
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::transitionup(::grpc::ServerContext *context,
                                               const ::coordinator::TransitionUpCMD *request,
                                               ::coordinator::RequestResult *response) {
        std::unique_lock ulk1(m_fsimage_mtx);
        std::unique_lock ulk2(m_stripeupdatingcount_mtx);

        //stop the world
        using ClusterLoc = std::tuple<int, int, int>;
        // {stripeid, fromnode, tonode}
        using MigrationPlan = std::vector<std::tuple<int, std::string, std::string>>;
        // {stripeid, fromnodes, codingnodes, tonodes}
        using XORCodingPlan = std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>>;
        using LRCCodingPlan = std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>>;

        using ForwardingPlan = std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>>;;
        coordinator::TransitionUpCMD_MODE mode = request->mode();
        if (mode == coordinator::TransitionUpCMD_MODE_BASIC) {
            //pick one cluster and one node as a gp-node in that cluster (simplified , just pick one from original g nodes in odd stripe)
            auto[migration_plans, coding_plans] = generate_basic_transition_plan(m_fs_image);
            std::cout << "transition up in basic mode ...." << std::endl;
            //display plans
            for (const auto &migration_plan:migration_plans) {
                int thisstripe = std::get<0>(migration_plan);
                const auto &src = std::get<1>(migration_plan);
                const auto &dst = std::get<2>(migration_plan);
                std::cout << "for new stripe " << thisstripe << std::endl;
                std::cout << "stripe" << thisstripe + 1 << "migrated out : " << std::endl;
                for (const auto &out : src) {
                    std::cout << out << "\t";
                }
                std::cout << std::endl;
                std::cout << "stripe" << thisstripe << "migrated in : " << std::endl;
                std::cout << "migrated in : " << std::endl;
                for (const auto &in : dst) {
                    std::cout << in << "\t";
                }
                std::cout << std::endl;
            }


            int g = 1;
            //removing old global blocks
            //then perform coding job
            for (int i = 0; i < coding_plans.size(); ++i) {
                g = std::get<3>(coding_plans[i]).size() + 1;
                int stripe_id = std::get<0>(coding_plans[i]);
                bool res1 = delete_global_parity_of(stripe_id);
                bool res2 = delete_global_parity_of(stripe_id + 1);

                const auto &fromuris = std::get<1>(coding_plans[i]);
                const auto &workinguri = std::get<2>(coding_plans[i]);
                const auto &touris = std::get<3>(coding_plans[i]);
                datanode::OP op;

                std::cout << "perform coding job  ..." << std::endl;

                //let working node know just forward to port ...+12220
                std::cout << "ask others to prepared to receive calculated block from worker ..." << std::endl;
                for (const auto &to : touris) {
                    if (stripe_in_updatingcounter.contains(stripe_id)) {
                        stripe_in_updatingcounter[stripe_id]++;
                    } else {
                        stripe_in_updatingcounter[stripe_id] = 1;
                    }
                    grpc::ClientContext askhandlingpushctx;
                    datanode::UploadCMD uploadCmd;
                    datanode::RequestResult handlingpushres;
                    std::cout << "ask " << to << " to wait from port +12220" << std::endl;
                    auto status = m_dn_ptrs[to]->handleupload(&askhandlingpushctx, uploadCmd, &handlingpushres);
                    if (!status.ok()) {
                        std::cout << to << "can not serve push \n";
                        return grpc::Status::CANCELLED;
                    }
                    std::cout << to << " prepared !" << std::endl;
                    op.add_to(to);
                }
                for (int j = 0; j < fromuris.size(); ++j) {
                    op.add_from(fromuris[j]);
                    int thisstripe = stripe_id;
                    grpc::ClientContext askpushctx;
                    datanode::DownloadCMD downloadCmd;
                    datanode::RequestResult pushres;
//                    datanode::OP op1;
//   //                 op1.set_index(j);
//                    op1.set_op(datanode::OP_CODEC_LRC);
//                    op1.add_to(workinguri);
                    if (2 * j < fromuris.size()) {
//                        op1.set_stripeid(stripe_id);
//                        stripeId.set_stripeid(stripe_id);
                    } else {
//                        op1.set_stripeid(stripe_id + 1);
//                        stripeId.set_stripeid(stripe_id+1);
                        thisstripe = stripe_id + 1;
                    }
                    //no from uri so dn can decide it is a push command
                    //should not delete , just normal download [denote by op_codec_lrc mode]
//                    std::cout << "ask " << fromuris[j] << " to preparing " << thisstripe
//                              << " push to[without deletion] " << workinguri << "+22221+" << j * 23 << std::endl;
//                    auto status2 = m_dn_ptrs[fromuris[j]]->pull_perform_push(&askpushctx, op1, &pushres);

                    auto status2 = m_dn_ptrs[fromuris[j]]->handledownload(&askpushctx, downloadCmd, &pushres);
                    if (!status2.ok()) {
                        std::cout << fromuris[j] << "can not push \n";
                        return grpc::Status::CANCELLED;
                    }
                    std::cout << fromuris[j] << " preprared !" << std::endl;
                }
                op.set_op(datanode::OP_CODEC_LRC);
                op.set_stripeid(stripe_id);
                grpc::ClientContext pppctx;
                datanode::RequestResult pppres;
                std::cout << "ask " << workinguri
                          << " to wait blocks and calculate new global parities and forward ..."
                          << std::endl;


                auto status = m_dn_ptrs[workinguri]->pull_perform_push(&pppctx, op, &pppres);
                if (!status.ok()) {
                    std::cout << workinguri << "perform ppp operation failed!\n";
                    m_cn_logger->error("perform ppp operation failed!\n");

                    return status;
                }
                //modify metadata
                //wait for updatingcondition
                while (stripe_in_updatingcounter.contains(stripe_id)) {
                    m_updatingcond.wait(ulk2, [&]() -> bool {
                        return !stripe_in_updatingcounter.contains(stripe_id);
                    });
                }

                std::cout << "complete basic transition coding plan for stripe " << stripe_id << " and "
                          << stripe_id + 1 << std::endl;
            }

            //migration all stripes
            for (const auto &nodepair : migration_plans) {
                std::vector<std::string> xor_nodes;
                std::vector<std::string> xor_src_nodes;
                int stripeid = std::get<0>(nodepair);
                const auto &fromuris = std::get<1>(nodepair);
                const auto &touris = std::get<2>(nodepair);
                std::unordered_set<std::string> skipset(fromuris.cbegin(), fromuris.cend());
                for (int j = 0; j < fromuris.size(); ++j) {
                    if (0 != j && 0 == j % g) {
                        grpc::ClientContext deletectx;
                        datanode::StripeId stripeId;
                        stripeId.set_stripeid(stripeid + 1);
                        datanode::RequestResult deleteres;
                        auto status = m_dn_ptrs[fromuris[j]]->clearstripe(&deletectx, stripeId, &deleteres);
                        std::cout << "ask " << fromuris[j] << "just delete block" << stripeid + 1 << std::endl;
                        continue;
                    }//skip local parity just delete
                    grpc::ClientContext uploadctx;
                    datanode::UploadCMD uploadCmd;
                    //migration pull nodes should set from
                    datanode::RequestResult uploadres;
                    auto handlepullstatus = m_dn_ptrs[touris[j]]->handleupload(&uploadctx, uploadCmd,
                                                                               &uploadres);
                    if (!handlepullstatus.ok()) {
                        std::cout << touris[j] << "can not handling push request!\n";
                        m_cn_logger->error("{} can not handling push request!", touris[j]);
                        return handlepullstatus;
                    }
                    if (stripe_in_updatingcounter.contains(stripeid)) {
                        stripe_in_updatingcounter[stripeid]++;
                    } else {
                        stripe_in_updatingcounter[stripeid] = 1;
                    }
                }
                //ask src node to upload[with deletion] to dst node
                for (int j = 0; j < touris.size(); ++j) {
                    if ((0 == (j % g)) && 0 != j) {
                        xor_nodes.push_back(touris[j]);
                    } else {
                        xor_src_nodes.push_back(touris[j]);
                        grpc::ClientContext pushctx;
                        datanode::OP op;
                        op.set_op(datanode::OP_CODEC_NO);//so dn konw it is a migration task
                        op.add_to(touris[j]);//so dn konw it is a push command with delete
                        op.set_stripeid(stripeid + 1);//stripe+1 for migration out
//                    op.set_index(0);
                        datanode::RequestResult askforpushres;
                        auto alivestatus = m_dn_ptrs[fromuris[j]]->pull_perform_push(&pushctx, op,
                                                                                     &askforpushres);
                        if (!alivestatus.ok()) {
                            std::cout << fromuris[j] << "can not handling push request!\n";
                            m_cn_logger->error("{} can not handling push request!", fromuris[j]);
                            return alivestatus;
                        }
                    }
                }


                //wait for migration condition
                while (stripe_in_updatingcounter.contains(stripeid)) {
                    m_updatingcond.wait(ulk2, [&]() {
                        return !stripe_in_updatingcounter.contains(stripeid);
                    });
                }


                //begin xor plan
                std::cout << "migration completed! then perform xoring\n";
                std::cout << "xoring task :" << xor_nodes.size() << std::endl;

                if (xor_nodes.size()) stripe_in_updatingcounter[stripeid] = xor_nodes.size();
                for (int j = 0; j < xor_nodes.size(); ++j) {
                    grpc::ClientContext xorctx;
                    datanode::OP op;
                    datanode::RequestResult xorres;
                    for (int i = 0; i < g; ++i) {
                        grpc::ClientContext handlepullctx;
                        datanode::RequestResult handlepullres;
                        datanode::DownloadCMD handlepullCmd;
                        m_dn_ptrs[xor_src_nodes[j * g + i]]->handledownload(&handlepullctx, handlepullCmd,
                                                                            &handlepullres);
                        op.add_from(xor_src_nodes[j * g + i]);
                    }
                    op.set_stripeid(stripeid);
                    op.set_op(datanode::OP_CODEC_XOR);
                    std::cout << "ask " << xor_nodes[j] << "to perform xor work for new block" << stripeid << std::endl;
                    m_dn_ptrs[xor_nodes[j]]->pull_perform_push(&xorctx, op, &xorres);
                }

                //wait for xor finished
                while (stripe_in_updatingcounter.contains(stripeid)) {
                    m_updatingcond.wait(ulk2, [&]() {
                        return !stripe_in_updatingcounter.contains(stripeid);
                    });
                }


                //modify metainfo

                bool res3 = rename_block_to(stripeid + 1, stripeid,
                                            skipset);//rename all originally valid placed block

                if (res3)
                    std::cout << "complete basic transition migration plan for stripe " << stripeid << " and "
                              << stripeid + 1 << std::endl;

            }//for each successive stripepair

            for (int i = 0; i < migration_plans.size(); ++i) {
                refreshfilesystemimagebasic(coding_plans[i], migration_plans[i]);
            }
            response->set_trueorfalse(true);
            return grpc::Status::OK;
        } else if (mode == coordinator::TransitionUpCMD_MODE_BASIC_PART) {


        } else {
            const auto &coding_plans = generate_designed_transition_plan(m_fs_image);

            for (
                auto &plan
                    : coding_plans) {
                grpc::ClientContext pppctx;
                datanode::RequestResult pppres;
                datanode::OP op;
                const auto &workingnode = std::get<3>(plan);
                const auto &fromuris = std::get<2>(plan);
                const auto &touris = std::get<4>(plan);
                int stripeid = std::get<0>(plan);
                int shift = std::get<1>(plan);
                op.set_op(datanode::OP_CODEC_REUSE);
                op.set_stripeid(stripeid);
                op.add_multiby(shift);
                for (int i = 0; i < fromuris.size(); ++i) {
                    op.add_from(fromuris[i]);
                }
                for (const auto &node: touris) {
                    grpc::ClientContext handlingpullctx;
                    datanode::RequestResult handlingpullres;
//so working_node only need to forward to 12220
                    datanode::UploadCMD uploadCmd;
                    grpc::Status status2;
                    status2 = m_dn_ptrs[node]->handleupload(&handlingpullctx, uploadCmd, &handlingpullres);
                    if (!status2.ok()) {
                        std::cout << "ask for pull failed\n";
                        m_cn_logger->warn("{} handling pull failed", node);
                        return status2;
                    }
                    op.add_to(node);
                }
                grpc::Status status;
                status = m_dn_ptrs[workingnode]->pull_perform_push(&pppctx, op, &pppres);
                if (!status.ok()) {
                    std::cout << "perform ppp failed\n";
                    m_cn_logger->warn("{} perform ppp failed ", workingnode);
                    return status;
                }
                for (int i = 0; i < fromuris.size(); ++i) {
                    grpc::ClientContext handlingpushctx;
                    datanode::RequestResult handlingpushres;
                    datanode::OP op2;
                    grpc::Status status2;
                    if (2 * i < fromuris.size()) {
                        op2.set_stripeid(stripeid);
                    } else {
                        op2.set_stripeid(stripeid + 1);
                    }
                    op2.set_op(datanode::OP_CODEC_REUSE);
                    op2.add_to(workingnode);
//                    op2.set_index(i);
                    status2 = m_dn_ptrs[fromuris[i]]->pull_perform_push(&handlingpushctx, op2, &handlingpushres);
                    if (!status2.ok()) {
                        std::cout << "ask for push failed\n";
                        m_cn_logger->warn("{} handling push failed", fromuris[i]);
                        return status;
                    }
                }

                std::unordered_set<std::string> skipset;//no skip
//rename old stripeid+1 to stripeid
                rename_block_to(stripeid
                                + 1, stripeid, skipset);

                std::cout << "complete designed transition plan for stripe " << stripeid << " and "
                          << stripeid + 1 <<
                          std::endl;
            }
            std::cout << "transition over\n";
//modify metainfo
//...
            return grpc::Status::OK;
        }
        return grpc::Status::OK;
    }

    std::pair<std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string >>>,
            std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>>>
    FileSystemCN::FileSystemImpl::generate_basic_transition_plan(
            std::unordered_map<int, std::vector<std::string>> &fsimage) {

        std::vector<std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>>> ret1;
        std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string >>> ret2;
        //todo load balance and priority schedule
        auto extractor = [&](const std::vector<std::string> &stripelocs) ->
                std::tuple<std::unordered_set<int>, std::unordered_set<int>, std::unordered_set<int>> {
            std::unordered_set<int> datacluster;
            std::unordered_set<int> globalcluster;
            bool flag = false;// following will be gp cluster
            int i = 0;
            for (; i < stripelocs.size(); ++i) {
                if (stripelocs[i] != "d" && stripelocs[i] != "l") {

                    datacluster.insert(m_dn_info[stripelocs[i]].clusterid);
                } else {
                    if (stripelocs[i] == "l") break;
                    i++;
                    continue;
                }
            }
            i++;
            for (; i < stripelocs.size(); ++i) {
                if (stripelocs[i] != "g") {
                    globalcluster.insert(m_dn_info[stripelocs[i]].clusterid);
                } else {
                    break;
                }
            }

            return {datacluster, {}, globalcluster};
        };

        int totalcluster = m_cluster_info.size();
        std::vector<int> total(totalcluster, 0);
        std::iota(total.begin(), total.end(), 0);
        for (int i = 1; i < fsimage.size(); i += 2) {
            auto kpos = std::find(fsimage[i - 1].cbegin(), fsimage[i - 1].cend(), "d");
            auto lpos = std::find(kpos, fsimage[i - 1].cend(), "l");
            auto gpos = std::find(lpos, fsimage[i - 1].cend(), "g");

            int k = kpos - fsimage[i - 1].cbegin();
            int l = lpos - kpos - 1;
            int g = gpos - lpos - 1;
            std::random_shuffle(total.begin(), total.end());
            auto[excluded, _ignore1, candg]=extractor(fsimage[i - 1]);
            auto[currentk, _ignore2, currentg]=extractor(fsimage[i]);
            std::vector<int> candcluster;//cand data cluster
            std::vector<int> overlap;
            std::vector<std::string> overlap_d_nodes;
            std::vector<std::string> overlap_l_nodes;
            std::vector<std::string> to_d_nodes;
            int u = 0;
            std::unordered_set<int> consideronce;
            for (; u < k; ++u) {
                int c = m_dn_info[fsimage[i][u]].clusterid;
                if (excluded.contains(c) || candg.contains(c)) {
                    if (!consideronce.contains(c)) {
                        overlap.push_back(c);
                        consideronce.insert(c);
                    }
                    overlap_d_nodes.push_back(fsimage[i][u]);
                }
            }
            ++u;
            for (; fsimage[i][u] != "l"; ++u) {
                int c = m_dn_info[fsimage[i][u]].clusterid;
                if (excluded.contains(c) || candg.contains(c)) {
                    if (!consideronce.contains(c)) {
                        overlap.push_back(c);
                        consideronce.insert(c);
                    }
                    overlap_l_nodes.push_back(fsimage[i][u]);
                }
            }

            for (int j = 0; j < totalcluster && overlap.size() > candcluster.size(); ++j) {
                if (!excluded.contains(total[j]) && !candg.contains(total[j])) {
                    candcluster.push_back(total[j]);
                }
            }

            //pick k datanodes l localparity nodes g globalparity nodes...
            //for this plan generator just pick 1 globalparity from candg , encoding, and forward to g-1 others
            auto target_g_cands = std::vector<std::string>(m_cluster_info[*candg.cbegin()].datanodesuri);
            std::random_shuffle(target_g_cands.begin(), target_g_cands.end());
            auto &target_coding_nodeuri = target_g_cands.front();
            std::vector<std::string> to_g_nodes(target_g_cands.cbegin() + 1, target_g_cands.cbegin() + g);
            std::vector<std::string> from_d_nodes(fsimage[i - 1].cbegin(), kpos);
            from_d_nodes.insert(from_d_nodes.end(),
                                fsimage[i].cbegin(),
                                std::find(fsimage[i].cbegin(), fsimage[i].cend(), "d"));

            std::vector<std::string> overlap_nodes;
            int idx = 0;
            while (idx < overlap_l_nodes.size()) {
                overlap_nodes.insert(overlap_nodes.end(),
                                     overlap_d_nodes.begin() + idx * k,
                                     overlap_d_nodes.begin() + (idx + 1) * k);
                overlap_nodes.push_back(overlap_l_nodes[idx]);
                std::vector<std::string> thiscluster(m_cluster_info[candcluster[idx]].datanodesuri);
                idx++;
                std::random_shuffle(thiscluster.begin(), thiscluster.end());
                to_d_nodes.insert(to_d_nodes.end(), thiscluster.cbegin(),
                                  thiscluster.cbegin() + k + 1);//at lease k+1 nodes !
            }


            ret1.push_back(
                    std::make_tuple(i - 1, std::move(from_d_nodes), target_coding_nodeuri, std::move(to_g_nodes)));
            ret2.push_back(
                    std::make_tuple(i - 1, std::move(overlap_nodes), std::move(to_d_nodes))
            );

        }
        return {ret2, ret1};
    }

    std::vector<std::tuple<int, int, std::vector<std::string>, std::string, std::vector<std::string>>>
    FileSystemCN::FileSystemImpl::generate_designed_transition_plan(
            std::unordered_map<int, std::vector<std::string>> &fsimage) {
        std::vector<std::tuple<int, int, std::vector<std::string>, std::string, std::vector<std::string>>> ret;

        auto extractor = [&](const std::vector<std::string> &stripelocs) ->
                std::tuple<std::unordered_set<int>, std::unordered_set<int>, std::unordered_set<int>> {
            std::unordered_set<int> datacluster;
            std::unordered_set<int> globalcluster;
            bool flag = false;// following will be gp cluster
            int i = 0;
            for (; i < stripelocs.size(); i++) {
                if (stripelocs[i] != "d" && stripelocs[i] != "l") {

                    datacluster.insert(m_dn_info[stripelocs[i]].clusterid);
                } else {
                    if (stripelocs[i] == "l") break;
                    i++;
                    continue;
                }
            }
            i++;
            for (; i < stripelocs.size(); ++i) {
                if (stripelocs[i] != "g") {
                    globalcluster.insert(m_dn_info[stripelocs[i]].clusterid);
                } else {
                    break;
                }
            }

            return {datacluster, {}, globalcluster};
        };

        for (int i = 1; i < fsimage.size(); i += 2) {
            auto kpos = std::find(fsimage[i - 1].cbegin(), fsimage[i - 1].cend(), "d");
            auto lpos = std::find(kpos, fsimage[i - 1].cend(), "l");
            auto gpos = std::find(lpos, fsimage[i - 1].cend(), "g");

            int k = kpos - fsimage[i - 1].cbegin();
            int l = lpos - kpos - 1;
            int g = gpos - lpos - 1;
            int shift = l * ceil(log2(g + 1));

            auto[_ignore1, _ignore2, candg] =extractor(fsimage[i - 1]);
            assert(candg.size() == 1);
            auto cand_g_nodes = m_cluster_info[*candg.cbegin()].datanodesuri;
            std::random_shuffle(cand_g_nodes.begin(), cand_g_nodes.end());
            //pick a worker
            auto target_coding_node = cand_g_nodes.front();
            std::vector<std::string> to_g_nodes(cand_g_nodes.begin() + 1, cand_g_nodes.begin() + g);
            std::vector<std::string> from_g_nodes(lpos + 1, gpos);
            from_g_nodes.insert(from_g_nodes.end(), std::find(fsimage[i].cbegin(), fsimage[i].cend(), "l") + 1,
                                std::find(fsimage[i].cbegin(), fsimage[i].cend(), "g"));

            ret.push_back(
                    std::make_tuple(i - 1, shift, std::move(from_g_nodes), target_coding_node,
                                    std::move(to_g_nodes)));
        }

        return ret;
    }

    bool FileSystemCN::FileSystemImpl::delete_global_parity_of(int stripeid) {
        auto it = std::find(m_fs_image[stripeid].cbegin(), m_fs_image[stripeid].cend(), "l");
        ++it;
        grpc::Status status;
        for (; *it != "g"; ++it) {
            grpc::ClientContext deletectx;
            datanode::RequestResult deleteres;
            datanode::StripeId stripeId;
            stripeId.set_stripeid(stripeid);
            std::cout << "delete " << *it << " global parity blocks of stripe : " << stripeid << std::endl;
            status = m_dn_ptrs[*it]->clearstripe(&deletectx, stripeId, &deleteres);
        }
        return true;
    }

    bool FileSystemCN::FileSystemImpl::rename_block_to(int oldstripeid, int newstripeid,
                                                       const std::unordered_set<std::string> &skipset) {
        //rename if exist otherwise nop
        for (const auto &node:m_fs_image[oldstripeid]) {
            if (node == "d" || skipset.contains(node)) continue;
            if (node == "l") break;
            std::cout << "rename " << node << " old stripe : " << oldstripeid << " to new stripe : " << newstripeid
                      << std::endl;
            grpc::ClientContext renamectx;
            datanode::RenameCMD renameCMD;
            renameCMD.set_oldid(oldstripeid);
            renameCMD.set_newid(newstripeid);
            datanode::RequestResult renameres;
            m_dn_ptrs[node]->renameblock(&renamectx, renameCMD, &renameres);
        }
        return true;
    }

    grpc::Status
    FileSystemCN::FileSystemImpl::setplacementpolicy(::grpc::ServerContext *context,
                                                     const ::coordinator::SetPlacementPolicyCMD *request,
                                                     ::coordinator::RequestResult *response) {
        if (coordinator::SetPlacementPolicyCMD_PLACE_RANDOM == request->place()) {
            m_placementpolicy = PLACE::RANDOM;
        } else if (coordinator::SetPlacementPolicyCMD_PLACE_COMPACT == request->place()) {
            m_placementpolicy = PLACE::COMPACT;
        } else {
            m_placementpolicy = PLACE::SPARSE;
        }
    }

    bool FileSystemCN::FileSystemImpl::refreshfilesystemimagebasic(
            const std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string >> &codingplan,
            const std::tuple<int, std::vector<std::string>, std::vector<std::string >> &migrationplan) {
        //copy stripe datanodes and put stripe+1 datanodes into a set
        //replace stripe+1 nodes set ∩ migration src nodes with migration dst nodes
        //merge stripe+1 set with stripe set
        //set gp nodes with worker node and forwarding dst nodes
        int modified_stripe = std::get<0>(codingplan) + 1;
        int k = std::get<1>(codingplan).size();//new k
        int g = std::get<3>(codingplan).size() + 1;
        int l = k / g;
        const auto &be_migrated = std::get<1>(migrationplan);
        const auto &dst_migrated = std::get<2>(migrationplan);
        const auto &total_node = std::get<1>(codingplan);
        const auto &new_gp_node = std::get<3>(codingplan);
        std::vector<std::string> new_stripelocation(k + 1 + l + 1 + g + 1, "");
        std::unordered_map<std::string, std::string> be_migratedset_to_dst;
        int j = 0;
        for (int i = 0; i < be_migrated.size(); ++i) {
            be_migratedset_to_dst[be_migrated[i]] = dst_migrated[i];
        }
        std::vector<std::string> total_datanodeset;
        for (int i = 0; i < total_node.size() / 2; ++i) {
            new_stripelocation[j++] = total_node[i];
        }
        for (int i = total_node.size() / 2; i < total_node.size(); ++i) {
            //all datanodes in both stripe
            if (be_migratedset_to_dst.contains(total_node[i])) {
                new_stripelocation[j++] = be_migratedset_to_dst[total_node[i]];
            } else {
                new_stripelocation[j++] = total_node[i];
            }
        }
        new_stripelocation[j++] = "d";
        std::vector<std::string> total_lpnodeset;
        for (int i = k / 2 + 1; "l" != m_fs_image[modified_stripe - 1][i]; ++i) {
            //stayed stripe lp
            new_stripelocation[j++] = m_fs_image[modified_stripe - 1][i];
        }
        for (int i = k / 2 + 1; "l" != m_fs_image[modified_stripe][i]; ++i) {
            //stayed stripe lp
            if (!be_migratedset_to_dst.contains(m_fs_image[modified_stripe][i]))
                new_stripelocation[j++] = m_fs_image[modified_stripe][i];
            else new_stripelocation[j++] = be_migratedset_to_dst[m_fs_image[modified_stripe][i]];
        }
        new_stripelocation[j++] = "l";
        new_stripelocation[j++] = std::get<2>(codingplan);
        for (const auto &gp:new_gp_node) {
            new_stripelocation[j++] = gp;
        }
        new_stripelocation[j++] = "g";
        m_fs_image.erase(modified_stripe - 1);
        m_fs_image.erase(modified_stripe);
        m_fs_image.insert({modified_stripe - 1, std::move(new_stripelocation)});
        return true;
    }

    bool FileSystemCN::FileSystemImpl::refreshfilesystemimagebasicpartial(
            const std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>> &codingplan,
            const std::tuple<int, std::vector<std::string>, std::vector<std::string >> &migrationplan) {

    }

    bool FileSystemCN::FileSystemImpl::refreshfilesystemimagedesigned(
            const std::tuple<int, std::vector<std::string>, std::string, std::vector<std::string>> &codingplan,
            const std::tuple<int, std::vector<std::string>, std::vector<std::string >> &migrationplan) {
        //simplest
        //merge both stripe datanodes and lp nodes
        //reset gp nodes

        std::vector<std::string> new_stripelocation;
        int modified_stripe = std::get<0>(codingplan) + 1;
        const auto &unmodified_location2 = m_fs_image[modified_stripe];
        const auto &unmodified_location1 = m_fs_image[modified_stripe - 1];

        auto d_marker1 = std::find(unmodified_location1.cbegin(), unmodified_location1.cend(), "d");
        auto l_marker1 = std::find(unmodified_location1.cbegin(), unmodified_location1.cend(), "l");
        auto d_marker2 = std::find(unmodified_location2.cbegin(), unmodified_location2.cend(), "d");
        auto l_marker2 = std::find(unmodified_location2.cbegin(), unmodified_location2.cend(), "l");
        for_each(unmodified_location1.begin(), d_marker1, [&](const std::string &loc) {
            new_stripelocation.push_back(loc);
        });
        for_each(unmodified_location2.begin(), d_marker2, [&](const std::string &loc) {
            new_stripelocation.push_back(loc);
        });
        for_each(d_marker1 + 1, l_marker1, [&](const std::string &loc) {
            new_stripelocation.push_back(loc);
        });
        for_each(d_marker2 + 1, l_marker2, [&](const std::string &loc) {
            new_stripelocation.push_back(loc);
        });

        const auto &forwarding_gp_node = std::get<3>(codingplan);
        new_stripelocation.emplace_back(std::get<2>(codingplan));
        new_stripelocation.insert(new_stripelocation.end(), forwarding_gp_node.cbegin(), forwarding_gp_node.cend());

        m_fs_image.erase(modified_stripe);
        m_fs_image.erase(modified_stripe - 1);

        m_fs_image.insert({modified_stripe - 1, std::move(new_stripelocation)});

        return true;
    }

    FileSystemCN::FileSystemImpl::Transition_Plan
    FileSystemCN::FileSystemImpl::generate_transition_plan(const std::vector<SingleStripeLayout> &layout,
                                                           const std::tuple<int, int, int> &para_before,
                                                           const std::tuple<int, int, int> &para_after,
                                                           int from,
                                                           bool partial_gp,
                                                           bool partial_lp,
                                                           int step) {
        std::vector<Partial_Coding_Plan> p1;
        std::vector<Global_Coding_Plan> p2;
        std::vector<Partial_Coding_Plan> p4;
        auto[k, l, g] = para_before;
        auto[_k, _l, _g]= para_after;
        int r = k / l;
        std::unordered_set<int> union_cluster;
        std::unordered_set<int> candgp_cluster;
        std::unordered_set<int> residue_cluster;
        std::unordered_set<int> overlap_cluster;
        std::unordered_set<int> to_cluster;
        std::unordered_set<int> gp_cluster;
        std::unordered_set<int> partial_cluster;

        std::iota(candgp_cluster.begin(), candgp_cluster.end(), 0);
        std::iota(to_cluster.begin(), to_cluster.end(), 0);
        int cluster_cap = 0;
        int residuecluster_cap = 0;
        //clusterid,{dn1,stripe1i,blkid1j},{dn2,stripeid2i,blk2j},...
        std::unordered_map<int, std::unordered_map<std::string, std::tuple<int, int, TYPE>>> cluster_tmp_total;
        std::unordered_map<int, std::unordered_map<std::string, std::tuple<int, int, TYPE>>> residuecluster_tmp_total;
        std::unordered_map<int, std::unordered_map<std::string, std::tuple<int, int, TYPE>>> cluster_tmp_datablock;//differentiate partial decoding or naive decoding

        for (int i = 0; i < step; ++i) {
            auto[dataloc, lploc, gploc] = layout[from + i];
            for (const auto &ci:gploc) {
                const auto &[_dnuri, _stripegrp] = ci;
                int _clusterid = m_dn_info[_dnuri].clusterid;
                gp_cluster.insert(_clusterid);
                candgp_cluster.insert(_clusterid);
            }
        }
        for (int i = 0; i < step; ++i) {
            auto[dataloc, lploc, gploc] = layout[from + i];
            for (const auto &ci:lploc) {
                const auto &[_dnuri, _stripegrp] = ci;
                int _clusterid = m_dn_info[_dnuri].clusterid;
                union_cluster.insert(_clusterid);
                residue_cluster.insert(_clusterid);
                to_cluster.erase(_clusterid);
                candgp_cluster.erase(_clusterid);
            }
        }
        for (int i = 0; i < step; ++i) {
            auto[dataloc, lploc, gploc] = layout[from + i];
            for (const auto &ci:dataloc) {
                const auto &[_dnuri, _stripegrp] = ci;
                int _clusterid = m_dn_info[_dnuri].clusterid;
                union_cluster.insert(_clusterid);
                candgp_cluster.erase(_clusterid);
                to_cluster.erase(_clusterid);
            }
        }


        std::unordered_map<int, std::vector<std::string>> migration_mapping;
        std::vector<int> migration_stripe;
        std::vector<std::string> migration_from;
        std::vector<std::string> migration_to;


        if (r <= g) {
            cluster_cap = _g;

            //migration
            for (int i = 0; i < step; ++i) {
                const auto &[dataloc, lploc, gploc] = layout[from + i];
                for (const auto &p:dataloc) {
                    const auto &[_dnuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _flag] = _stripegrp;
                    int ci = m_dn_info[_dnuri].clusterid;
                    if (0 != cluster_tmp_total.count(ci) && cluster_tmp_total[ci].size() < cluster_cap) {
                        cluster_tmp_total[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                    } else if (0 != cluster_tmp_total.count(ci)) {
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        migration_stripe.emplace_back(from + i);
                        migration_from.emplace_back(_dnuri);
                        migration_mapping.emplace(ci, std::vector<std::string>());
                    } else {
                        //unseen cluster
                        cluster_tmp_total.emplace(ci, std::unordered_map<std::string, std::tuple<int, int, TYPE>>());
                        cluster_tmp_datablock.emplace(ci,
                                                      std::unordered_map<std::string, std::tuple<int, int, TYPE>>());
                        cluster_tmp_total[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                    }
                }
            }

            for (int i = 0; !partial_lp && i < step; ++i) {
                const auto &[dataloc, lploc, gploc] = layout[from + i];
                for (const auto &p:lploc) {
                    const auto &[_lpuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _flag] = _stripegrp;
                    int ci = m_dn_info[_lpuri].clusterid;
                    if (0 != cluster_tmp_total.count(ci) && cluster_tmp_total[ci].size() >= cluster_cap) {
                        migration_stripe.emplace_back(from + i);
                        migration_from.emplace_back(_lpuri);
                    }
                }
            }


            //reconsx
            std::vector<int> stripeids, blkids;
            std::vector<std::string> fromdns;
            std::vector<std::string> todns_globalplan;
            //pick gp nodes
            int pickedgpcluster = -1;
            std::sample(candgp_cluster.cbegin(), candgp_cluster.cend(), &pickedgpcluster, 1,
                        std::mt19937{std::random_device{}()});
            to_cluster.erase(pickedgpcluster);
            auto candgps = m_cluster_info[pickedgpcluster].datanodesuri;
            std::sample(candgps.cbegin(), candgps.cend(), todns_globalplan.begin(), _g,
                        std::mt19937{std::random_device{}()});


            const auto &worker = todns_globalplan[0];

            for (const auto &c:cluster_tmp_datablock) {
                if (partial_gp && c.second.size() > _g) {
                    //need partial
                    for (int j = 0; j < _g; ++j) {
                        for (const auto &clusterdninfo:c.second) {
                            const auto &[_dnuri, _blkinfo] = clusterdninfo;
                            const auto &[_stripeid, _blkid, _type] = _blkinfo;
                            fromdns.emplace_back(_dnuri);
                            stripeids.emplace_back(_stripeid);
                            blkids.emplace_back(_blkid);
                        }
                        auto gatewayuri = m_cluster_info[c.first].gatewayuri;
                        p1.emplace_back(std::make_tuple(stripeids, blkids, fromdns, gatewayuri));
                        //clear
                        fromdns.clear();
                        stripeids.clear();
                        blkids.clear();
                    }
                } else {
                    //or send data blocks to gp cluster gateway as a special partial plan
                    for (const auto &clusterdninfo:c.second) {
                        const auto &[_dnuri, _blkinfo] = clusterdninfo;
                        const auto &[_stripeid, _blkid, _blktype] = _blkinfo;
                        stripeids.push_back(_stripeid);
                        blkids.push_back(_blkid);
                        fromdns.emplace_back(_dnuri);
                        p1.emplace_back(std::make_tuple(stripeids, blkids, fromdns, worker));
                        fromdns.clear();
                        stripeids.clear();
                        blkids.clear();
                    }
                }
            }


            for (int j = 0; j < _g; ++j) {

                for (const auto &c:cluster_tmp_datablock) {
                    if (partial_gp && c.second.size() > _g) {
                        //already partial coded , stored on that cluster's gateway
                        auto gatewayuri = m_cluster_info[c.first].gatewayuri;
                        fromdns.emplace_back(gatewayuri);
                        stripeids.emplace_back(from);
                        blkids.emplace_back(-j);
                    } else {
                        //or send data blocks to
                        for (const auto &clusterdninfo:c.second) {
                            const auto &[_dnuri, _blkinfo] = clusterdninfo;
                            const auto &[_stripeid, _blkid, _blktype] = _blkinfo;
                            fromdns.emplace_back(worker);//from gp cluster gateway
                            stripeids.emplace_back(_stripeid);
                            blkids.emplace_back(_blkid);
                        }

                    };
                }
                p2.emplace_back(std::make_tuple(stripeids, blkids, fromdns, todns_globalplan[j]));
                stripeids.clear();
                blkids.clear();
                fromdns.clear();
            }


            auto tocluster_iter = to_cluster.begin();
            std::string *todns_iter = nullptr;
            int tmpc = -1;
            int lastc = -1;
            //just migration
            std::vector<std::string> to_partialfrom;
            for (int j = 0; j < migration_from.size(); ++j) {
                int srcci = m_dn_info[migration_from[j]].clusterid;
                if (partial_lp && 0 != j && 0 == j % r) {
                    const auto &partworker = *todns_iter;
                    ++todns_iter;
                    p4.emplace_back(std::make_tuple(std::vector<int>(to_partialfrom.size(), from),
                                                    std::vector<int>(to_partialfrom.size(), -1), to_partialfrom,
                                                    partworker));
                    to_partialfrom.clear();
                }

                if (srcci != lastc) {
                    todns_iter = nullptr;
                    lastc = srcci;
                    tmpc = *tocluster_iter;
                    ++tocluster_iter;
                }

                if (nullptr == todns_iter) {
                    todns_iter = &m_cluster_info[tmpc].datanodesuri[0];
                }
                migration_to.emplace_back(*todns_iter);
                if (partial_lp) {
                    to_partialfrom.emplace_back(*todns_iter);
                }
                ++todns_iter;
            }
            if (!to_partialfrom.empty()) {
                const auto &partworker = *todns_iter;
                ++todns_iter;
                p4.emplace_back(std::make_tuple(std::vector<int>(to_partialfrom.size(), from),
                                                std::vector<int>(to_partialfrom.size(), -1), to_partialfrom,
                                                partworker));
            }

            Migration_Plan p3 = std::make_tuple(migration_stripe, migration_from, migration_to);
            return {p1, p2, p3, p4};
        } else if (0 == (r % (g + 1))) {
            cluster_cap = _g + (_g / g);
            //migration
            for (int i = 0; i < step; ++i) {
                const auto &[dataloc, lploc, gploc] = layout[from + i];
                for (const auto &p:dataloc) {
                    const auto &[_dnuri, _stripegrp] = p;
                    const auto &[_blkid, _type, _flag] = _stripegrp;
                    int ci = m_dn_info[_dnuri].clusterid;
                    if (0 != cluster_tmp_total.count(ci) && cluster_tmp_total[ci].size() < cluster_cap) {
                        cluster_tmp_total[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                    } else if (0 != cluster_tmp_total.count(ci)) {
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        migration_stripe.emplace_back(from + i);
                        migration_from.emplace_back(_dnuri);
                        migration_mapping.emplace(ci, std::vector<std::string>());
                    } else {
                        //unseen cluster
                        cluster_tmp_total.emplace(ci, std::unordered_map<std::string, std::tuple<int, int, TYPE>>());
                        cluster_tmp_datablock.emplace(ci,
                                                      std::unordered_map<std::string, std::tuple<int, int, TYPE>>());
                        cluster_tmp_total[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                        cluster_tmp_datablock[ci].emplace(_dnuri, std::make_tuple(from + i, _blkid, TYPE::DATA));
                    }
                }


            }


            //reconsx
            std::vector<int> stripeids, blkids;
            std::vector<std::string> fromdns;
            std::vector<std::string> todns_globalplan;
            //pick gp nodes
            int pickedgpcluster = -1;
            int skiplp = 0;
            for (auto ci:residue_cluster) {
                if (candgp_cluster.contains(ci)) {
                    if (pickedgpcluster != -1) pickedgpcluster = ci;
                    skiplp += l;
                }
            }

            if (pickedgpcluster != -1) {
                std::sample(candgp_cluster.cbegin(), candgp_cluster.cend(), &pickedgpcluster, 1,
                            std::mt19937{std::random_device{}()});
            }
            to_cluster.erase(pickedgpcluster);
            auto candgps = m_cluster_info[pickedgpcluster].datanodesuri;
            std::sample(candgps.cbegin(), candgps.cend(), todns_globalplan.begin(), _g + _l - skiplp,
                        std::mt19937{std::random_device{}()});


            const auto &worker = todns_globalplan[0];

            for (const auto &c:cluster_tmp_datablock) {
                if (partial_gp && c.second.size() > _g) {
                    //need partial
                    for (int j = 0; j < _g; ++j) {
                        for (const auto &clusterdninfo:c.second) {
                            const auto &[_dnuri, _blkinfo] = clusterdninfo;
                            const auto &[_stripeid, _blkid, _type] = _blkinfo;
                            fromdns.emplace_back(_dnuri);
                            stripeids.emplace_back(_stripeid);
                            blkids.emplace_back(_blkid);
                        }
                        auto gatewayuri = m_cluster_info[c.first].gatewayuri;
                        p1.emplace_back(std::make_tuple(stripeids, blkids, fromdns, gatewayuri));
                        //clear
                        fromdns.clear();
                        stripeids.clear();
                        blkids.clear();
                    }
                } else {
                    //or send data blocks to gp cluster gateway as a special partial plan
                    for (const auto &clusterdninfo:c.second) {
                        const auto &[_dnuri, _blkinfo] = clusterdninfo;
                        const auto &[_stripeid, _blkid, _blktype] = _blkinfo;
                        stripeids.push_back(_stripeid);
                        blkids.push_back(_blkid);
                        fromdns.emplace_back(_dnuri);
                        p1.emplace_back(std::make_tuple(stripeids, blkids, fromdns, worker));
                        fromdns.clear();
                        stripeids.clear();
                        blkids.clear();
                    }
                }
            }


            for (int j = 0; j < _g; ++j) {

                for (const auto &c:cluster_tmp_datablock) {
                    if (partial_gp && c.second.size() > _g) {
                        //already partial coded , stored on that cluster's gateway
                        auto gatewayuri = m_cluster_info[c.first].gatewayuri;
                        fromdns.emplace_back(gatewayuri);
                        stripeids.emplace_back(from);
                        blkids.emplace_back(-j);
                    } else {
                        //or send data blocks to
                        for (const auto &clusterdninfo:c.second) {
                            const auto &[_dnuri, _blkinfo] = clusterdninfo;
                            const auto &[_stripeid, _blkid, _blktype] = _blkinfo;
                            fromdns.emplace_back(worker);//from gp cluster gateway
                            stripeids.emplace_back(_stripeid);
                            blkids.emplace_back(_blkid);
                        }

                    };
                }
                p2.emplace_back(std::make_tuple(stripeids, blkids, fromdns, todns_globalplan[j]));
                stripeids.clear();
                blkids.clear();
                fromdns.clear();
            }
           


            auto tocluster_iter = to_cluster.begin();
            std::string *todns_iter = nullptr;
            int tmpc = -1;
            int lastc = -1;
            //just migration
            std::vector<std::string> to_partialfrom;
            for (int j = 0; j < migration_from.size(); ++j) {
                int srcci = m_dn_info[migration_from[j]].clusterid;
                if (partial_lp && 0 != j && 0 == j % r) {
                    const auto &worker = *todns_iter;
                    ++todns_iter;
                    p4.emplace_back(std::make_tuple(std::vector<int>(to_partialfrom.size(), from),
                                                    std::vector<int>(to_partialfrom.size(), -1), to_partialfrom,
                                                    worker));
                }
                migration_to.emplace_back(*todns_iter);
                ++todns_iter;

                if (srcci != lastc) {
                    todns_iter = nullptr;
                    lastc = srcci;
                    tmpc = *tocluster_iter;
                    ++tocluster_iter;
                }

                if (nullptr == todns_iter) {
                    todns_iter = &m_cluster_info[tmpc].datanodesuri[0];
                }
                if (partial_lp) {
                    to_partialfrom.emplace_back(*todns_iter);
                    ++todns_iter;
                }
            }
            if (!to_partialfrom.empty()) {
                const auto &worker = *todns_iter;
                ++todns_iter;
                p4.emplace_back(std::make_tuple(std::vector<int>(to_partialfrom.size(), from),
                                                std::vector<int>(to_partialfrom.size(), -1), to_partialfrom, worker));
            }

            Migration_Plan p3 = std::make_tuple(migration_stripe, migration_from, migration_to);
            return {p1, p2, p3, p4};
        } else {
            cluster_cap = _g + (_g / g);
            int m = (k / l) % (g + 1);//residue_datablock per group
            residuecluster_cap = (_g / m) * (m + 1);//count lp
            int mix_cap = _g + 1 + (g / m);
            //pick up residue cluster id
            unordered_map<int, vector<int>> cluster_precount;
            for (auto ci:union_cluster) {
                cluster_precount.insert({ci, vector<int>(step, 0)});
            }
            for (int i = 0; i < step; ++i) {
                auto[dataloc, lploc, gploc] = layout[from + i];
                for (auto ci:dataloc) {
                    cluster_precount[ci][i]++;
                }
                for (auto ci:lploc) {
                    cluster_precount[ci][i]++;
                }
            }

            unordered_map<int, unordered_set<int>> special_cluster_mover;
            unordered_set<int> mixpack_cluster;
            for (auto stat:cluster_precount) {
                auto[c, countlist] = stat;
                set<int> dedup(countlist.cbegin(), countlist.cend());
                if (dedup.size() > 1) {
                    //mixed
                    mixpack_cluster.insert(c);
                }
                int cap = *max_element(countlist.cbegin(), countlist.cend());
                if (accumulate(countlist.cbegin(), countlist.cend(), 0) <= (mix_cap)) {
                    continue;
                }
                if (residue_cluster.contains(c)) {
                    if (cap > g + 1) {
                        if (!dedup.contains(g + 1))// ==2 but all residue
                        {
                            continue;
                        } else {
                            // ==2 one normal one residue , move normal
                            if (!special_cluster_mover.contains(c)) {
                                special_cluster_mover.insert(
                                        {c, unordered_set < int > {}});
                            }
                            for (int i = 0; i < step; ++i) {
                                if (countlist[i] == g + 1) {
                                    special_cluster_mover[c].insert(i);
                                }
                            }
                        }
                    } else if (cap == (g + 1)) {
                        if (!special_cluster_mover.contains(c)) {
                            special_cluster_mover.insert(
                                    {c, unordered_set < int > {}});
                        }
                        for (int i = 0; i < step; ++i) {
                            if (countlist[i] != g + 1) {
                                special_cluster_mover[c].insert(i);
                            }
                        }
                    } else {
                        // can be concat
                    }
                }

            }

            vector <pair<int, int>> migration_plan;
            for (int i = 0; i < step; ++i) {
                auto[dataloc, lploc, gploc] = layout[from + i];
                for (int j = 0; j < dataloc.size(); ++j) {
                    int ci = dataloc[j];
                    if (0 != cluster_tmp.count(ci)) {
                        cluster_tmp_datablock[ci]++;
                        if (special_cluster_mover.contains(ci) && special_cluster_mover[ci].contains(i)) {
                            migration_plan.emplace_back(from + i, j);
                            continue;
                        }
                        int cap = (0 == residue_cluster.count(ci)) ? cluster_cap : residuecluster_cap;
                        if (mixpack_cluster.contains(ci)) cap = mix_cap;
                        if (cluster_tmp[ci] < cap) {
                            cluster_tmp[ci]++;
                        } else {
                            migration_plan.emplace_back(from + i, j);
                        }
                    } else {
                        cluster_tmp.insert({ci, 1});
                        cluster_tmp_datablock.insert({ci, 1});
                        if (special_cluster_mover.contains(ci) && special_cluster_mover[ci].contains(i)) {
                            migration_plan.emplace_back(from + i, j);
                            continue;
                        }
                    }
                }

                for (int j = 0; j < lploc.size(); ++j) {
                    int ci = lploc[j];
                    if (special_cluster_mover.contains(ci) && special_cluster_mover[ci].contains(i)) {
                        migration_plan.emplace_back(from + i, j + k);
                        continue;
                    }
                    int cap = (0 == residue_cluster.count(ci)) ? cluster_cap : residuecluster_cap;
                    if (mixpack_cluster.contains(ci)) cap = mix_cap;
                    if (cluster_tmp[ci] < cap) {
                        cluster_tmp[ci]++;
                    } else {
                        migration_plan.emplace_back(from + i, j + k);
                    }
                }

            }
            //reconsx
            for (auto c:cluster_tmp_datablock) {
                if (partial_gp) rec += min(c.second, _g);
                else rec += c.second;
            }
            mig = migration_plan.size();
            return {rec, mig};
        }


    }


//CN in DN implementation
/*
grpc::Status FileSystemCN::CoordinatorImpl::reportblocktransfer(::grpc::ServerContext *context,
                                                                const ::coordinator::StripeId *request,
                                                                ::coordinator::RequestResult *response) {
    m_fsimpl_ptr->getMCnLogger()->info("datanode {} receive block of stripe {} from client!",context->peer(),request->stripeid());
    m_fsimpl_ptr->updatestripeupdatingcounter(request->stripeid(),context->peer());//should be a synchronous method or atomic int;
    response->set_trueorfalse(true);
    return grpc::Status::OK;
}*/
    FileSystemCN::CoordinatorImpl::CoordinatorImpl() {}

    const std::shared_ptr<FileSystemCN::FileSystemImpl> &FileSystemCN::CoordinatorImpl::getMFsimplPtr() const {
        return m_fsimpl_ptr;
    }

    void FileSystemCN::CoordinatorImpl::setMFsimplPtr(const std::shared_ptr<FileSystemImpl> &mFsimplPtr) {
        m_fsimpl_ptr = mFsimplPtr;
    }

}