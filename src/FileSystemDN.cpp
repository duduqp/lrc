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
#include "ToolBox.h"

namespace lrc {

    grpc::Status lrc::FileSystemDN::FromCoordinatorImpl::handleblocktransfer(::grpc::ServerContext *context,
                                                                             const ::datanode::StripeId *request,

                                                                             ::datanode::RequestResult *response) {


        std::cout << "try prepare acceptor" << std::endl;
        std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> holdme = get_sharedholder();

        auto acptptr = prepareacceptor(*this);

        if (acptptr) std::cout << "prepared acceptor" << std::endl;
        else {
            std::cout << "prepared acceptor failed!" << std::endl;
        }
        auto sockptr = std::make_unique<asio::ip::tcp::socket>(m_ioservice);
        int blocksize = 64 * 1024 * 1024;
        std::cout << "default blk size:" << m_defaultblocksize << std::endl;
        auto handler = [_sockptr = std::move(
                sockptr), _id = request->stripeid(), ptr = std::move(acptptr), defaultblksize = m_defaultblocksize](
                std::string datadir, int total_blksize, int stripeid,
                std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> _this) mutable {
            //move in the outer uniqueptr,it's safe
            std::cout << "handle recv thread start" << std::endl;
            ptr->accept(*_sockptr);
            std::vector<char> buf(1024 * 1024 * defaultblksize);
            std::cout << "accept client connection" << std::endl;
            asio::read(*_sockptr, asio::buffer(buf, defaultblksize * 1024 * 1024));
            std::cout << "receive a block" << std::endl;
            if (!std::filesystem::exists(std::filesystem::path{datadir})) std::filesystem::create_directory(datadir);
            std::cout << datadir + std::to_string(_id) << std::endl;
            std::ofstream ofs(datadir + std::to_string(_id), std::ios::binary | std::ios::out | std::ios::trunc);
            ofs.write(&buf[0], 1024 * 1024 * 64);
            std::cout << "successfully write : " << ofs.tellp() << "bytes" << std::endl;
            ofs.flush();
            grpc::ClientContext reportctx;
            coordinator::StripeId stripeId;
            stripeId.set_stripeid(stripeid);
            coordinator::RequestResult reportres;
            std::cout << "datanode call reportblktransfer to cn!\n";
            auto status = _this->m_fs_stub->reportblocktransfer(&reportctx, stripeId, &reportres);
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
            std::thread h(std::move(handler), m_datapath, blocksize, request->stripeid(), holdme);
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
        std::cout << "hook——————————————————————————————————————————" << std::endl;
        if (!std::filesystem::exists(std::filesystem::path(m_datapath))) {
            std::cout << "hook——————————————————————————————————————————" << std::endl;
            std::filesystem::create_directory(std::filesystem::path(m_datapath));
        }
        std::cout << "hook——————————————————————————————————————————" << std::endl;
        auto res = initialize();
        std::cout << "hook——————————————————————————————————————————" << std::endl;
        if (!res) {
            m_dnfromcnimpl_logger->error("datanode fromcnimpl initialize failed!");
            return;
        }
        std::cout << "hook——————————————————————————————————————————" << std::endl;
        res = initstub();
        std::cout << "hook——————————————————————————————————————————" << std::endl;
        if (!res) {
            m_dnfromcnimpl_logger->error("datanode cnstub initialize failed!");
            return;
        }
        std::cout << "hook——————————————————————————————————————————" << std::endl;
        m_dnfromcnimpl_logger->info("datanode cnstub initialize success!");
        m_initialized = true;

    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDnfromcnUri() const {
        return m_dnfromcn_uri;
    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDatatransferPort() const {
        return m_datatransfer_port;
    }

    const std::string &FileSystemDN::FromCoordinatorImpl::getMDatapath() const {
        return m_datapath;
    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::clearstripe(::grpc::ServerContext *context, const ::datanode::StripeId *request,
                                                   ::datanode::RequestResult *response) {

        m_dnfromcnimpl_logger->info("clear stripe {}", request->stripeid());
        std::filesystem::remove(m_datapath.append(std::to_string(request->stripeid())));
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

    void FileSystemDN::FromCoordinatorImpl::setMDatatransferPort(const std::string &mDatatransferPort) {
        m_datatransfer_port = mDatatransferPort;
    }

    void FileSystemDN::FromCoordinatorImpl::setMDatapath(const std::string &mDatapath) {
        m_datapath = mDatapath;
    }

    const std::shared_ptr<spdlog::logger> &FileSystemDN::FromCoordinatorImpl::getMDnfromcnimplLogger() const {
        return m_dnfromcnimpl_logger;
    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::renameblock(::grpc::ServerContext *context, const::datanode::RenameCMD *request,
                                                   ::datanode::RequestResult *response) {
        if(std::filesystem::exists(m_datapath+std::to_string(request->oldid()))){
            std::filesystem::rename(m_datapath+std::to_string(request->oldid()),m_datapath+std::to_string(request->newid()));
        }else{
            std::cout << "old stripeid : " <<request->newid()<<" currently not exists" << std::endl;
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
            } else if (std::string{"datanodetransfer_port"} == propname) {
                m_datatransfer_port = propvalue;
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
        m_cnfromdn_stub = std::move(coordinator::FromDataNode::NewStub(grpc::CreateChannel(
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
    FileSystemDN::FromCoordinatorImpl::handleblockpull(::grpc::ServerContext *context,
                                                       const ::datanode::StripeId *request,
                                                       ::datanode::RequestResult *response) {
        std::cout << "try prepare acceptor" << std::endl;
        std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> holdme = get_sharedholder();

        auto acptptr = prepareacceptor(*this);
        std::cout << "prepared acceptor" << std::endl;

        auto sockptr = std::make_unique<asio::ip::tcp::socket>(m_ioservice);
        int blocksize = 64 * 1024 * 1024;
        std::cout << "default blk size:" << m_defaultblocksize << std::endl;
        auto handler = [_sockptr = std::move(
                sockptr), _id = request->stripeid(), ptr = std::move(acptptr), defaultblksize = m_defaultblocksize](
                std::string datadir, int total_blksize, int stripeid,
                std::shared_ptr<lrc::FileSystemDN::FromCoordinatorImpl> _this) mutable {
            //move in the outer uniqueptr,it's safe
            std::cout << "handle send thread start" << std::endl;
            std::vector<char> buf(1024 * 1024 * defaultblksize);
            ptr->accept(*_sockptr);
            std::cout << "accept client connection" << std::endl;
            if (!std::filesystem::exists(std::filesystem::path{datadir})) {
                _this->m_dnfromcnimpl_logger->warn("stripe not exists!");
                std::cout << "stripe not exists!" << std::endl;
                return;
            }
            std::ifstream ifs(datadir + std::to_string(_id), std::ios::binary | std::ios::in);
            ifs.seekg(0);
            ifs.read(&buf[0], 1024 * 1024 * 64);
            std::cout << "successfully read : " << ifs.tellg() << "bytes from " << datadir + std::to_string(_id)
                      << std::endl;
            asio::write(*_sockptr, asio::buffer(buf, defaultblksize * 1024 * 1024));
            std::cout << "successfully send !\n";

        };

        try {
            std::thread h(std::move(handler), m_datapath, blocksize, request->stripeid(), holdme);
            h.detach();
        } catch (std::exception &e) {
            std::cout << e.what() << std::endl;
        }
        std::cout << "download prepared!\n";
        response->set_trueorfalse(true);
        return grpc::Status::OK;
    }

    grpc::Status
    FileSystemDN::FromCoordinatorImpl::pull_perform_push(::grpc::ServerContext *context, const ::datanode::OP *request,
                                                         ::datanode::RequestResult *response) {
        auto mode = request->op();
        auto holdme = get_sharedholder();
        int srcnums = request->from_size();
        int tonums = request->to_size();
        int stripeid = request->stripeid();
        int index = request->index();
        auto reusedpushtask = [&](char *res, const std::string &dst,
                int _defaultblocksize) {
            asio::io_context pullctx;
            asio::ip::tcp::socket sock(pullctx);

            int datatransferport_offset = 12220;//12220 for normal[handlingtransfer] , 12221 for migration[pulltask]
            auto ipaddr = uritoipaddr(dst);
            auto port = std::stoi(dst.substr(dst.find(':') + 1)) + datatransferport_offset;
            asio::ip::tcp::endpoint ep(asio::ip::address_v4::from_string(ipaddr), port);
            sock.connect(ep);

            //pull a packet to gpbuffer[index]
            asio::write(sock, asio::buffer(res, _defaultblocksize * 1024 * 1024));

            std::cout << "push a block to" << dst << std::endl;
        };
        auto reusedpulltask = [&](char **res, const std::string &src, int index,bool beremove = false) {
            asio::io_context pullctx;
            asio::ip::tcp::socket sock(pullctx);

            int datatransferport_offset = beremove ? 12221 :12220;//12220 for normal , 12221 for transition
            auto ipaddr = uritoipaddr(src);
            auto port = std::stoi(src.substr(src.find(':') + 1)) + datatransferport_offset;
            asio::ip::tcp::endpoint ep(asio::ip::address_v4::from_string(ipaddr), port);
            sock.connect(ep);

            //pull a packet to gpbuffer[index]
            asio::read(sock, asio::buffer(res[index], m_defaultblocksize * 1024 * 1024));

            std::cout << "pull a block from" << src << std::endl;
        };

        auto pulltask = [&](std::shared_ptr<FileSystemDN::FromCoordinatorImpl> _this, std::string src,
                            std::string _datapath,char * target_region,
                            int _defaultblocksize,
                            int _stripeid,int index,
                            bool beflush = true) {
            asio::io_context pushctx;
            asio::ip::tcp::socket sock(pushctx);
            const auto &localipaddr = src;
            auto ipaddr = uritoipaddr(localipaddr);
            short port = std::stoi(localipaddr.substr(localipaddr.find(':') + 1)) + 12221+index;
            sock.connect(asio::ip::tcp::endpoint(asio::ip::address_v4::from_string(ipaddr), port));

            std::vector<char> res(_defaultblocksize * 1024 * 1024);
            asio::read(sock, asio::buffer(&res[0], _defaultblocksize * 1024 * 1024));

            if (beflush) {
                std::string blkpath(_datapath + std::to_string(_stripeid));
                std::ofstream ofs(blkpath);
                ofs.write(&res[0], _defaultblocksize * 1024 * 1024);
                ofs.flush();
                std::cout << "succesfully flush to local\n";
            }else{
                //copy to target region
                std::copy(res.cbegin(),res.cend(),target_region);
                std::cout << "succesfully copy to target region\n";

            }
            std::cout << "successfully pull a block\n";

        };

        auto preparingpush = [&](std::shared_ptr<FileSystemDN::FromCoordinatorImpl> _this,
                                 std::string _dnfromcn_uri, std::string _datapath,
                                 int _defaultblocksize, int stripeid,int index,
                                 bool beremove = true) {
            asio::io_context pushctx;
            asio::ip::tcp::acceptor acpt(pushctx);
            const auto &localipaddr = _dnfromcn_uri;
            auto ipaddr = uritoipaddr(localipaddr);
            short port = std::stoi(localipaddr.substr(localipaddr.find(':') + 1)) + 12221+index;
            acpt.bind(asio::ip::tcp::endpoint(asio::ip::address_v4::from_string(ipaddr), port));
            acpt.listen();
            std::cout << "prepareed push thread\n";
            std::string blkpath{_datapath + std::to_string(stripeid)};
            if (std::filesystem::exists(blkpath)) {
                std::cout << "block not exists\n";
                return;
            }

            std::vector<char> blk(_defaultblocksize * 1024 * 1024);
            std::ifstream ifs(blkpath);
            ifs.read(&blk[0], _defaultblocksize * 1024 * 1024);
            std::cout << "read block size: " << ifs.tellg() << std::endl;
            asio::ip::tcp::socket sock(pushctx);
            acpt.accept(sock);

            asio::write(sock, asio::buffer(&blk[0], _defaultblocksize * 1024 * 1024));

            //delete original
            if (beremove) {
                std::filesystem::remove(blkpath);
                std::cout << "remove block " << stripeid << std::endl;
            }
        };
        if (mode == datanode::OP_CODEC_NO) {
            //just pull a block or push a block , note : means delete original
            //push or pull
            if (0 == tonums) {
                //pull mode

                std::thread t(pulltask,holdme,request->from(0),m_datapath,nullptr,m_defaultblocksize,
                        stripeid,0,true);

                t.detach();
            } else if(0==srcnums){
                //push mode
                //same as handlingpull but once connection over , delete original block
                //index = 0
                //push local stripe stripeid
                std::thread t(preparingpush, holdme,
                              m_dnfromcn_uri, m_datapath,
                              m_defaultblocksize, stripeid,0,true);
                t.detach();
            }

        } else {
            if (mode == datanode::OP_CODEC_LRC) {
                if(0!=srcnums) {
                    //worker
                    auto ecschema = std::make_tuple(srcnums, static_cast<int>(srcnums / (tonums + 1)), tonums);
                    LRCCoder coder(ecschema);
                    char **gpbuffer = new char *[srcnums];
                    char **res = new char *[tonums];
                    for (int i = 0; i < srcnums; ++i) {
                        gpbuffer[i] = new char[m_defaultblocksize * 1024 * 1024];//packet
                    }
                    for (int i = 0; i < tonums+1; ++i) {
                        res[i] = new char[m_defaultblocksize * 1024 * 1024];
                    }

                    boost::thread_group tp;
                    for (int i = 0; i < srcnums; ++i) {
                        tp.create_thread(std::bind(pulltask, holdme,request->from(i),m_datapath,gpbuffer[i],
                                m_defaultblocksize,(2*i<srcnums?stripeid:stripeid+1),i,false));
                    }
                    tp.join_all();
                    //perform coding
                    int l = srcnums / (tonums + 1); //  2*(k' div g' = l')
                    int w = l * ceil(log2(tonums + 2));// w = 2*l'*...
                    coder.encode(gpbuffer, NULL, res, w, m_defaultblocksize * 1024 * 1024);
                    //push to ...+12220
                    boost::thread_group tp2;
                    for (int i = 1; i < tonums+1; ++i) {
                        tp2.create_thread(std::bind(reusedpushtask, res[i], request->to(i-1),m_defaultblocksize));
                    }
                    tp2.join_all();

                    std::ofstream ofs(m_datapath + std::to_string(stripeid));
                    ofs.write(res[0], m_defaultblocksize * 1024 * 1024);
                    ofs.flush();
                    std::cout << "successfully write new global pairty block in stripe " << request->stripeid()
                              << " to local\n";
                }else{
                    //no from node , it prepare to listen to 12221+index
                    //no deletion
                    //preparepush task for stripeid with listen port 12221+index = index
                    try{
                    std::thread t(preparingpush,holdme,m_dnfromcn_uri,m_datapath,m_defaultblocksize,stripeid,index,false);
                    t.detach();
                    } catch (...) {
                        std::cout << "error when create push thread\n"<<std::endl;
                    }

                }
            } else if (mode == datanode::OP_CODEC_REUSE) {
                if(0!=srcnums) {

                    int multiby = pow(2, request->shift());
                    char **gpbuffer = new char *[srcnums];
                    for (int i = 0; i < srcnums; ++i) {
                        gpbuffer[i] = new char[m_defaultblocksize * 1024 * 1024];//packet
                    }
                    /*
                     *
                     *  1 [q1=(2^shift)] q1^2  q1^3...
                     *  1 [q2=(2^shift)^2] q2^2 ...
                     *
                     */
                    int q = multiby;
                    std::vector<int> codingmatrix((srcnums / 2) * (srcnums));
                    char **res = new char *[tonums+1];
                    for (int i = 0; i < tonums+1; ++i) {
                        res[i] = new char[m_defaultblocksize * 1024 * 1024];
                        codingmatrix[i * (srcnums) + i] = 1;
                        codingmatrix[i * (srcnums) + i + (tonums+1)] = q;
                        q = galois_single_multiply(q, multiby, request->shift()*2);
                    }

                    boost::thread_group tp;
                    for (int i = 0; i < srcnums; ++i) {
                    //please no flush
                        tp.create_thread(std::bind(pulltask,holdme,request->from(i),m_datapath,gpbuffer[i],m_defaultblocksize, stripeid,i, false));
                    }

                    tp.join_all();
                    //perform reuse style computation
                    int *bitmatrix = jerasure_matrix_to_bitmatrix((srcnums), (srcnums / 2), request->shift() * 2,
                                                                  &codingmatrix[0]);
                    jerasure_bitmatrix_encode((srcnums), (srcnums / 2), (request->shift() * 2), bitmatrix, gpbuffer,
                                              res,
                                              m_defaultblocksize * 1024 * 1024, sizeof(long));
                    //forwarding global parities
                    boost::thread_group tp2;
                    for (int i = 0; i < tonums; ++i) {
                        tp2.create_thread(std::bind(reusedpushtask, res[i+1], request->to(i), m_defaultblocksize));
                    }
                    tp2.join_all();
                    //flush res[0] to local
                    std::ofstream ofs(m_datapath + std::to_string(stripeid));
                    ofs.write(res[0], m_defaultblocksize * 1024 * 1024);
                    ofs.flush();
                    std::cout << "successfully write new global pairty block in stripe " << request->stripeid()
                              << " to local\n";
                }else{
                    //srcnums == 0
                    //reuse mode but a pure push command
                    //with deletion
                    std::thread t(preparingpush,holdme,m_dnfromcn_uri,m_datapath,m_defaultblocksize,stripeid,index,true);
                    t.detach();
                }

            } else if (mode == datanode::OP_CODEC_XOR) { ;
            }
        }
        response->set_trueorfalse(true);
        return grpc::Status::OK;
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
        m_datatransfer_port = m_dn_fromcnimpl_ptr->getMDatatransferPort();

    }
}