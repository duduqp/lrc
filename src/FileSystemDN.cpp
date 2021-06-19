//
// Created by 杜清鹏 on 2021/4/1.
//
#include <datanode.grpc.pb.h>
#include <FileSystemDN.h>
#include <asio.hpp>
#include <grpc++/create_channel.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <boost/thread/thread.hpp>
#include <Jerasure/include/jerasure.h>
#include <erasurecoding/LRCCoder.h>
#include <boost/thread/barrier.hpp>
#include "ToolBox.h"

namespace lrc {

    grpc::Status lrc::FileSystemDN::FromCoordinatorImpl::handleupload(::grpc::ServerContext *context,
                                                                      const ::datanode::UploadCMD *request,

                                                                      ::datanode::RequestResult *response) {


        std::cout << "try prepare acceptor" << std::endl;
        std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> holdme = get_sharedholder();

        auto acptptr = prepareacceptor(*this, std::stoi(m_datanodeupload_port));

        if (acptptr) std::cout << "prepared acceptor" << std::endl;
        else {
            std::cout << "prepared acceptor failed!" << std::endl;
        }
        auto sockptr = std::make_unique<asio::ip::tcp::socket>(m_ioservice);
        int blocksize = m_defaultblocksize * 1024 * 1024;
        std::cout << "default blk size:" << m_defaultblocksize << std::endl;
        auto handler = [_sockptr = std::move(
                sockptr), ptr = std::move(acptptr), defaultblksize = m_defaultblocksize](
                std::string datadir, int total_blksize,
                std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> _this) mutable {
            //move in the outer uniqueptr,it's safe
            int id = -1;
            std::cout << "handle recv thread start" << std::endl;
            ptr->accept(*_sockptr);
            std::vector<char> buf(1024 * 1024 * defaultblksize);
            std::cout << "accept client connection" << std::endl;
            asio::read(*_sockptr, asio::buffer(&id, sizeof(id)));
            std::cout << "this connection will handle block " << id << std::endl;
            asio::read(*_sockptr, asio::buffer(buf, defaultblksize * 1024 * 1024));
            std::cout << "receive block" << id << std::endl;
            if (!std::filesystem::exists(std::filesystem::path{datadir})) std::filesystem::create_directory(datadir);
            std::cout << datadir + std::to_string(id) << std::endl;
            std::ofstream ofs(datadir + std::to_string(id), std::ios::binary | std::ios::out | std::ios::trunc);
            ofs.write(&buf[0], 1024 * 1024 * defaultblksize);
            std::cout << "successfully write : " << ofs.tellp() << "bytes" << std::endl;
            ofs.flush();
            grpc::ClientContext reportctx;
            coordinator::StripeId stripeId;
            stripeId.set_stripeid(id);
            coordinator::RequestResult reportres;
            std::cout << "datanode call reportblktransfer to cn!\n";
            auto status = _this->m_fs_stub->reportblockupload(&reportctx, stripeId, &reportres);
            std::cout << "datanode call reportblktransfer to cn success!\n";
            if (status.ok() && reportres.trueorfalse()) {
                std::cout << "report stripe transfer reach cn! " << std::endl;
                _this->m_dnfromcnimpl_logger->info("report stripe transfer reach cn!");

            } else {
                std::cout << "report stripe transfer not reach cn! " << std::endl;
                _this->m_dnfromcnimpl_logger->error("report stripe transfer not reach cn!");
            }
        };

        try {
            std::thread h(std::move(handler), m_datapath, blocksize, holdme);
            h.detach();
        } catch (std::exception &e) {
            std::cout << e.what() << std::endl;
        }
        std::cout << "receive askDNhandling rpc!\n";
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    grpc::Status lrc::FileSystemDN::FromCoordinatorImpl::clearallstripe(::grpc::ServerContext *context,
                                                                        const ::datanode::ClearallstripeCMD *request,
                                                                        ::datanode::RequestResult *response) {
        //clear dir mdatapth
        m_dnfromcnimpl_logger->info("clear data directory!");
        std::filesystem::remove_all(m_datapath);
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    FileSystemDN::FromCoordinatorImpl::FromCoordinatorImpl(
            const std::string &mConfPath,
            const std::string &mDatapath
    )
            : m_confpath(mConfPath), m_datapath(mDatapath) {
        m_dnfromcnimpl_logger = spdlog::basic_logger_mt("dncnimpl", "./log/logFile2.txt", true);
        if (!std::filesystem::exists(std::filesystem::path(m_datapath))) {
            std::filesystem::create_directory(std::filesystem::path(m_datapath));
        }
        auto res = initialize();
        if (!res) {
            m_dnfromcnimpl_logger->error("datanode fromcnimpl initialize failed!");
            return;
        }
        res = initstub();
        if (!res) {
            m_dnfromcnimpl_logger->error("datanode cnstub initialize failed!");
            return;
        }
        m_dnfromcnimpl_logger->info("datanode cnstub initialize success!");
        m_initialized = true;

    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDnfromcnUri() const {
        return m_dnfromcn_uri;
    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDatanodeUploadPort() const {
        return m_datanodeupload_port;
    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDatapath() const {
        return m_datapath;
    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::clearstripe(::grpc::ServerContext *context, const ::datanode::StripeId *request,
                                                   ::datanode::RequestResult *response) {

        m_dnfromcnimpl_logger->info("clear stripe {}", request->stripeid());
        std::filesystem::remove(m_datapath + (std::to_string(request->stripeid())));
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::checkalive(::grpc::ServerContext *context,
                                                  const ::datanode::CheckaliveCMD *request,
                                                  ::datanode::RequestResult *response) {
        if (m_initialized) {
            response->set_trueorfalse(true);
        } else {
            response->set_trueorfalse(false);
        }
        return grpc::Status::OK;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDnfromcnUri(const std::string &mDnfromcnUri) {
        m_dnfromcn_uri = mDnfromcnUri;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDatanodeuploadPort(const std::string &mDatanodeuploadPort) {
        m_datanodeupload_port = mDatanodeuploadPort;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDatanodedownloadPort(const std::string &mDatanodedownloadPort) {
        m_datanodedownload_port = mDatanodedownloadPort;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDatapath(const std::string &mDatapath) {
        m_datapath = mDatapath;
    }

    const std::shared_ptr<spdlog::logger> &FileSystemDN::FromCoordinatorImpl::getMDnfromcnimplLogger() const {
        return m_dnfromcnimpl_logger;
    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::renameblock(::grpc::ServerContext *context, const ::datanode::RenameCMD *request,
                                                   ::datanode::RequestResult *response) {
        if (std::filesystem::exists(m_datapath + std::to_string(request->oldid()))) {
            std::filesystem::rename(m_datapath + std::to_string(request->oldid()),
                                    m_datapath + std::to_string(request->newid()));
        } else {
            std::cout << "old stripeid : " << request->newid() << " currently not exists" << std::endl;
        }
        return grpc::Status::OK;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDnfromcnimplLogger(
            const std::shared_ptr<spdlog::logger> &mDnfromcnimplLogger) {
        m_dnfromcnimpl_logger = mDnfromcnimplLogger;
    }

    const asio::io_context &FileSystemDN::FromCoordinatorImpl::getMIoservice() const {
        return m_ioservice;
    }


    bool FileSystemDN::FromCoordinatorImpl::isInitialized() const {
        return m_initialized;
    }

    void FileSystemDN::FromCoordinatorImpl::setInitialized(bool initialized) {
        FromCoordinatorImpl::m_initialized = initialized;
    }

    bool FileSystemDN::FromCoordinatorImpl::initialize() {
        //parse /conf/configuration.xml
        //parse xml
        pugi::xml_document xdoc;
        xdoc.load_file(m_confpath.c_str(), pugi::parse_default, pugi::encoding_utf8);
        auto propertynode = xdoc.child("properties").child("property");

        for (auto propattr = propertynode.first_attribute(); propattr; propattr = propattr.next_attribute()) {
            auto propname = propattr.name();
            auto propvalue = propattr.value();
            if (std::string{"fs_uri"} == propname) {
                m_fs_uri = propvalue;
            } else if (std::string{"datanodeupload_port"} == propname) {
                m_datanodeupload_port = propvalue;
            } else if (std::string{"datanodedownload_port"} == propname) {
                m_datanodedownload_port = propvalue;
            } else if (std::string{"default_block_size"} == propname) {
                m_defaultblocksize = std::stoi(propvalue);
            } else if (std::string{"datanode_uri"} == propname) {
                m_dnfromcn_uri = propvalue;
            }
        }
        return true;
    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMFsUri() const {
        return m_fs_uri;
    }

    void FileSystemDN::FromCoordinatorImpl::setMFsUri(const std::string &mFsUri) {
        m_fs_uri = mFsUri;
    }

    int FileSystemDN::FromCoordinatorImpl::getMDefaultblocksize() const {
        return m_defaultblocksize;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDefaultblocksize(int mDefaultblocksize) {
        m_defaultblocksize = mDefaultblocksize;
    }

    bool FileSystemDN::FromCoordinatorImpl::initstub() {
        m_fs_stub = std::move(coordinator::FileSystem::NewStub(grpc::CreateChannel(
                m_fs_uri, grpc::InsecureChannelCredentials()
        )));

        return true;
    }

    std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> FileSystemDN::FromCoordinatorImpl::get_sharedholder() {
        return shared_from_this();
    }

    FileSystemDN::FromCoordinatorImpl::FromCoordinatorImpl() {

    }

    FileSystemDN::FromCoordinatorImpl::~FromCoordinatorImpl() {

    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::handledownload(::grpc::ServerContext *context,
                                                      const ::datanode::DownloadCMD *request,
                                                      ::datanode::RequestResult *response) {
        std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> holdme = get_sharedholder();
        auto acptptr = prepareacceptor(*this, std::stoi(m_datanodedownload_port));
        auto sockptr = std::make_unique<asio::ip::tcp::socket>(m_ioservice);
        int blocksize = m_defaultblocksize * 1024 * 1024;
        auto handler = [_sockptr = std::move(
                sockptr), ptr = std::move(acptptr), defaultblksize = m_defaultblocksize](
                std::string datadir, int total_blksize,
                std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> _this) mutable {
            //move in the outer uniqueptr,it's safe
            int id = -1;
            std::vector<char> buf(1024 * 1024 * defaultblksize);
            ptr->accept(*_sockptr);
            asio::read(*_sockptr, asio::buffer(&id, sizeof(id)));
            std::cout << "accept client connection for download block " << id << std::endl;
            if (!std::filesystem::exists(std::filesystem::path{datadir})) {
                _this->m_dnfromcnimpl_logger->warn("stripe not exists!");
                std::cout << "stripe not exists!" << std::endl;
                return;
            }
            std::ifstream ifs(datadir + std::to_string(id), std::ios::binary | std::ios::in);
            ifs.seekg(0);
            ifs.read(&buf[0], 1024 * 1024 * defaultblksize);
            std::cout << "successfully read : " << ifs.tellg() << "bytes from " << datadir + std::to_string(id)
                      << std::endl;
            asio::write(*_sockptr, asio::buffer(buf, defaultblksize * 1024 * 1024));
            std::cout << "successfully send !\n";
        };
        try {
            std::thread h(std::move(handler), m_datapath, blocksize, holdme);
            h.detach();
        } catch (std::exception &e) {
            std::cout << e.what() << std::endl;
        }
        std::cout << "download prepared!\n";
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDatanodeDownloadPort() const {
        return m_datanodedownload_port;
    }


    grpc::Status
    FileSystemDN::FromCoordinatorImpl::pull_perform_push(::grpc::ServerContext *context, const ::datanode::OP *request,
                                                         ::datanode::RequestResult *response) {
        auto mode = request->op();
        auto holdme = get_sharedholder();
        int srcnums = request->from_size();
        int tonums = request->to_size();
        int stripeid = request->stripeid();

        //delete
        int shift = 0;
        int idx = 0;


        std::cout << "mode" << mode << "\n";
        std::cout << "this thread" << holdme.get() << "\n";
        std::cout << "srcnums" << srcnums << "\n";
        std::cout << "tonums" << tonums << "\n";
        std::cout << "stripeid" << stripeid << "\n";
        std::shared_ptr<boost::barrier> oneshot_barrier(std::make_shared<boost::barrier>(srcnums + 1));
        std::shared_ptr<boost::barrier> receiving_barrier(std::make_shared<boost::barrier>(srcnums + 1));
        auto inmemorypushtask = [&](char *src, const std::string &dstip, short dstport,
                                    int _defaultblocksize,int stripe_id) {
            asio::io_context pushctx;
            asio::ip::tcp::socket sock(pushctx);

            std::cout << "will push in memory data to " << dstip << ":" << dstport << "as "<<stripeid<<std::endl;
            asio::ip::tcp::endpoint ep(asio::ip::address_v4::from_string(dstip), dstport);
            sock.connect(ep);
            std::cout << "connection established with " << dstip << ":" << dstport << std::endl;
            asio::write(sock, asio::buffer(&stripe_id,sizeof(stripe_id)));
            asio::write(sock, asio::buffer(src, _defaultblocksize * 1024 * 1024));

            std::cout << "successfully push a block to ipaddr:port" << dstip << ":" << dstport << std::endl;
        };

        auto pulltask = [](
                const std::string &pullingip, short pullingport,
                const std::string &_datapath, char *target_region,
                int _defaultblocksize, int _stripeid,
                bool beflush = false) {
            asio::io_context pullctx;
            asio::ip::tcp::socket sock(pullctx);
            std::cout << "thread prepare to pull from ipaddr:port" << pullingip << ":" << pullingport << std::endl;
            try {
                asio::ip::tcp::endpoint ep(asio::ip::address::from_string(pullingip), pullingport);
                sock.connect(ep);
                std::cout << "connected  ,will pull block " << _stripeid << " from  ip:" << pullingip << " port:"
                          << pullingport
                          << std::endl;
                asio::write(sock, asio::buffer(&_stripeid, sizeof(_stripeid)));
                std::vector<char> res(_defaultblocksize * 1024 * 1024);
                auto readn = asio::read(sock, asio::buffer(&res[0], _defaultblocksize * 1024 * 1024));
                std::cout << readn << " bytes block successfully pulled " << std::endl;
                if (beflush) {
                    std::string blkpath(_datapath + std::to_string(_stripeid));
                    std::ofstream ofs(blkpath);
                    ofs.write(&res[0], _defaultblocksize * 1024 * 1024);
                    ofs.flush();
                    std::cout << "succesfully flush to local\n";
                } else {
                    //copy to target region
                    std::copy(res.cbegin(), res.cend(), target_region);
                    std::cout << "succesfully copy to target region\n";

                }
                std::cout << "successfully pull a block\n";
            } catch (std::exception &e) {
                std::cout << e.what() << std::endl;
            }
        };

        auto pushtask = [&](const std::string &dstip, short port,
                            const std::string &_datapath,
                            int _defaultblocksize, int src_stripeid,int dst_stripeid,
                            bool beremove = true) {
            asio::io_context pushctx;
            asio::ip::tcp::endpoint endpoint(asio::ip::address_v4::from_string(dstip), port);
            asio::ip::tcp::socket sock(pushctx);
            std::cout << "thread prepare block " << src_stripeid << "will push to ipaddr:port" << dstip << ":" << port
                      <<"as" << "block "<<dst_stripeid <<std::endl;
            sock.connect(endpoint);
            std::cout << "connected to dst " << endpoint << std::endl;
            std::string blkpath{_datapath + std::to_string(src_stripeid)};
            if (!std::filesystem::exists(blkpath)) {
                std::cout << "block not exists\n";
                return;
            }
            try {
                std::vector<char> blk(_defaultblocksize * 1024 * 1024);
                std::ifstream ifs(blkpath);
                ifs.read(&blk[0], _defaultblocksize * 1024 * 1024);
                std::cout << "read local block size: " << ifs.tellg() << std::endl;
                asio::write(sock, asio::buffer(&dst_stripeid, sizeof(dst_stripeid)));
                asio::write(sock, asio::buffer(&blk[0], _defaultblocksize * 1024 * 1024));
                //delete original
                if (beremove) {
                    std::filesystem::remove(blkpath);
                    std::cout << "remove block " << stripeid << std::endl;
                }
            } catch (std::exception &e) {
                std::cout << e.what() << std::endl;
            }
        };
        if (mode == datanode::OP_CODEC_NO) {
            //just pull a block or push a block , note : means delete original
            //push or pull
            std::cout << m_dnfromcn_uri << "in migration status" << std::endl;
            if (0 == tonums && 0 == srcnums) {
                //impossible case ?
            } else if (1 == tonums && 0 == srcnums) {
                //push mode
                // migration same as handlingpull but once connection over , delete original block
                //index = 0
                //push local stripe stripeid
                const std::string &dsturi = request->to(0);
                const std::string &dstip = uritoipaddr(dsturi);
                short dstport = std::stoi(dsturi.substr(dsturi.find(':') + 1)) + 12220; // upload
                std::cout << m_dnfromcn_uri << "will push block " << stripeid << " to " << dstip << ":" << dstport
                          << std::endl;

                std::thread t(pushtask,
                              dstip, dstport,
                              m_datapath,
                              m_defaultblocksize, stripeid,stripeid-1,
                              true);
                t.detach();
            }
        } else {
            if (mode == datanode::OP_CODEC_LRC) {
                if (0 != srcnums) {
                    //worker
                    std::cout << m_dnfromcn_uri << " is a worker " << std::endl;
                    std::cout << m_dnfromcn_uri << "will perform coding job " << std::endl;
                    std::cout << "schema: " << srcnums << " " << static_cast<int>(srcnums / (tonums + 1)) << " "
                              << tonums + 1 << std::endl;
                    char **gpbuffer = new char *[srcnums];
                    char **res = new char *[tonums + 1];
                    for (int i = 0; i < srcnums; ++i) {
                        gpbuffer[i] = new char[m_defaultblocksize * 1024 * 1024];//packet
                    }
                    for (int i = 0; i < tonums + 1; ++i) {
                        res[i] = new char[m_defaultblocksize * 1024 * 1024];
                    }
                    //perform coding
                    std::cout << m_dnfromcn_uri << "will pull blocks first" << std::endl;
                    boost::thread_group tp;
                    for (int i = 0; i < srcnums; ++i) {
                        const auto &uri = request->from(i);
                        const auto &ip = uritoipaddr(uri);
                        short port = std::stoi(uri.substr(uri.find(':') + 1)) + 22220;//download
                        tp.create_thread(std::bind(pulltask,
                                                   ip, port,
                                                   m_datapath, gpbuffer[i],
                                                   m_defaultblocksize, (2 * i < srcnums ? stripeid : stripeid + 1),
                                                   false));
                    }

                    tp.join_all();
                    auto ecschema = std::make_tuple(srcnums, static_cast<int>(srcnums / (tonums + 1)),
                                                    tonums + 1);
                    LRCCoder coder(ecschema, true);
                    int l = srcnums / (tonums + 1); //  2*(k' div g' = l')
                    int w = l * ceil(log2(tonums + 2));// w = 2*l'*...
                    std::cout << "pulling blocks is completed , word length is : 2^" << w << std::endl;
                    coder.display_matrix();
                    coder.encode(gpbuffer, NULL, res, m_defaultblocksize * 1024 * 1024);
                    //push to ...+12220
                    std::cout << "coding job completed , forwarding to others " << std::endl;
                    boost::thread_group tp2;
                    for (int i = 1; i < tonums + 1; ++i) {
                        const std::string &uri = request->to(i - 1);
                        const std::string &dstip = uritoipaddr(uri);
                        short dstport = std::stoi(uri.substr(uri.find(':') + 1)) + 12220;

                        tp2.create_thread(std::bind(inmemorypushtask, res[i], dstip, dstport,m_defaultblocksize,stripeid));
                    }
                    tp2.join_all();

                    std::ofstream ofs(m_datapath + std::to_string(stripeid));
                    ofs.write(res[0], m_defaultblocksize * 1024 * 1024);
                    ofs.flush();
                    std::cout << "successfully write new global pairty block in stripe " << stripeid
                              << " to local\n";
                    for (int i = 0; i < srcnums; ++i) {
                        delete[] gpbuffer[i];
                    }
                    for (int i = 0; i < tonums + 1; ++i) {
                        delete[] res[i];
                    }
                    delete[] gpbuffer;
                    delete[] res;
                } else {
                    //impossible case ?
                }
            } else if (mode == datanode::OP_CODEC_REUSE) {
                if (0 != srcnums) {
                    int multiby = pow(2, shift);
                    char **gpbuffer = new char *[srcnums];
                    for (int i = 0; i < srcnums; ++i) {
                        gpbuffer[i] = new char[m_defaultblocksize * 1024 * 1024];//packet
                    }

                    //  1 [q1=(2^shift)] q1^2  q1^3...
                    // 1 [q2=(2^shift)^2] q2^2 ...


                    int q = multiby;
                    std::vector<int> codingmatrix((srcnums / 2) * (srcnums));
                    char **res = new char *[tonums + 1];
                    for (int i = 0; i < tonums + 1; ++i) {
                        res[i] = new char[m_defaultblocksize * 1024 * 1024];
                        codingmatrix[i * (srcnums) + i] = 1;
                        codingmatrix[i * (srcnums) + i + (tonums + 1)] = q;
                        q = galois_single_multiply(q, multiby, shift * 2);
                    }
                    boost::thread_group tp;
                    const std::string &localip = uritoipaddr(m_dnfromcn_uri);
                    short listeningport_offset = std::stoi(m_dnfromcn_uri.substr(m_dnfromcn_uri.find(':') + 1)) + 22221;
                    for (int i = 0; i < srcnums; ++i) {
                        //please no flush
                        short port = listeningport_offset + idx * 23;
                        tp.create_thread(std::bind(pulltask,
                                                   localip, port,
                                                   m_datapath, gpbuffer[i],
                                                   m_defaultblocksize, stripeid,
                                                   false));
                    }
                    tp.join_all();
                    //perform reuse style computation
                    int *bitmatrix = jerasure_matrix_to_bitmatrix((srcnums), (srcnums / 2), shift * 2,
                                                                  &codingmatrix[0]);
                    jerasure_bitmatrix_encode((srcnums), (srcnums / 2), shift * 2, bitmatrix, gpbuffer,
                                              res,
                                              m_defaultblocksize * 1024 * 1024, sizeof(long));
                    //forwarding global parities
                    boost::thread_group tp2;
                    for (int i = 0; i < tonums; ++i) {
                        const std::string &dsturi = request->to(i);
                        const std::string &dstip = uritoipaddr(dsturi);
                        short dstport = std::stoi(dsturi.substr(dsturi.find(':') + 1)) + 12220;
                        tp2.create_thread(std::bind(inmemorypushtask, res[i + 1],
                                                    dstip, dstport, m_defaultblocksize,stripeid));
                    }
                    tp2.join_all();
                    //flush res[0] to local
                    std::ofstream ofs(m_datapath + std::to_string(stripeid));
                    ofs.write(res[0], m_defaultblocksize * 1024 * 1024);
                    ofs.flush();
                    std::cout << "successfully write new global pairty block in stripe " << stripeid
                              << " to local\n";
                    for (int i = 0; i < srcnums; ++i) {
                        delete[] gpbuffer[i];
                    }
                    for (int i = 0; i < tonums + 1; ++i) {
                        delete[] res[i];
                    }
                    delete[] gpbuffer;
                    delete[] res;


                } else {
                    //impossible case ?
                }
            }else if(mode == datanode::OP_CODEC_XOR) {
                //perfrom pull and xor and store
                char **lpbuffer=new char*[srcnums];
                char *xorres=new char[m_defaultblocksize*1024*1024];
                std::fill(xorres,xorres+m_defaultblocksize*1024*1024,0);
                for(int i=0;i<srcnums;++i)
                {
                    lpbuffer[i]=new char[m_defaultblocksize*1024*1024];
                }

                boost::thread_group tp;
                for(int i=0;i<srcnums;++i)
                {
                    const auto & uri = request->from(i);
                    const auto & srcip = uritoipaddr(uri);
                    short srcport = std::stoi(uri.substr(uri.find(':')+1))+22220;
                    tp.create_thread(std::bind(pulltask,srcip,srcport,m_datapath,lpbuffer[i],
                            m_defaultblocksize,stripeid,false));
                }
                tp.join_all();

                //perform xoring
                for(int i =0;i<srcnums;++i)
                {
                    galois_region_xor(lpbuffer[i],xorres,m_defaultblocksize*1024*1024);
                }

                //flush to local
                std::ofstream ofs(m_datapath+std::to_string(stripeid));
                ofs.write(xorres,m_defaultblocksize*1024*1024);
                ofs.flush();
                std::cout << "successfully write "<<ofs.tellp()<<"bytes to local as block "<<stripeid<<std::endl;

                //start another thread !
                //keep alive
                auto reporttask = [_holdme=holdme,_stripeid=stripeid](){
                    grpc::ClientContext reportctx;
                    coordinator::StripeId stripeId;
                    stripeId.set_stripeid(_stripeid);
                    coordinator::RequestResult reportres;
                    _holdme->m_fs_stub->reportblockupload(&reportctx,stripeId,&reportres);
                };
                 std::thread report_t(reporttask);
                 report_t.detach();
            }
        }
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    //these method used for migration[with deletion]
    grpc::Status
    FileSystemDN::FromCoordinatorImpl::handlepull(::grpc::ServerContext *context,
                                                  const ::datanode::HandlePullCMD *request,
                                                  ::datanode::RequestResult *response) {
        short port = std::stoi(m_datanodeupload_port) + 20000;
        auto acptptr = prepareacceptor(*this, port);
        auto sockptr = std::make_unique<asio::ip::tcp::socket>(m_ioservice);
        auto handler = [_sockptr = std::move(sockptr), ptr = std::move(
                acptptr), defaultblksize = m_defaultblocksize, datadir = m_datapath]() {
            //move in the outer uniqueptr,it's safe
            int id = -1;
            std::vector<char> buf(1024 * 1024 * defaultblksize);
            ptr->accept(*_sockptr);
            asio::read(*_sockptr, asio::buffer(&id, sizeof(id)));
            std::cout << "accept client connection for download block " << id << std::endl;
            if (!std::filesystem::exists(std::filesystem::path{datadir})) {
                std::cout << "stripe not exists!" << std::endl;
                return;
            }
            std::ifstream ifs(datadir + std::to_string(id), std::ios::binary | std::ios::in);
            ifs.seekg(0);
            ifs.read(&buf[0], 1024 * 1024 * defaultblksize);
            std::cout << "successfully read : " << ifs.tellg() << "bytes from " << datadir + std::to_string(id)
                      << std::endl;
            asio::write(*_sockptr, asio::buffer(buf, defaultblksize * 1024 * 1024));
            std::filesystem::remove(datadir + std::to_string(id));
            std::cout << "successfully send and delete!\n";
        };

        //lambda catch a noncopyable but only movable sockptr [uniqueptr]
        std::thread t(std::move(handler));
        t.detach();


    }


    FileSystemDN::~FileSystemDN() {
    }

    FileSystemDN::FileSystemDN(const std::string mConfPath, const std::string mLogPath,
                               const std::string mDataPath) : m_conf_path(mConfPath), m_log_path(mLogPath),
                                                              m_data_path(mDataPath),
                                                              m_dn_fromcnimpl_ptr(
                                                                      FileSystemDN::FromCoordinatorImpl::getptr(
                                                                              mConfPath, mDataPath)) {

        m_dn_logger = spdlog::basic_logger_mt("datanode_logger", mLogPath, false);
        m_datanodeupload_port = m_dn_fromcnimpl_ptr->getMDatanodeUploadPort();
        m_datanodedownload_port = m_dn_fromcnimpl_ptr->getMDatanodeDownloadPort();

    }
}