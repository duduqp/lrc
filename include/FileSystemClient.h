//
// Created by 杜清鹏 on 2021/3/29.
//

#ifndef LRC_FILESYSTEMCLIENT_H
#define LRC_FILESYSTEMCLIENT_H



#include "devcommon.h"

#include "asio/thread_pool.hpp"
#include "pugixml.hpp"

#include <grpcpp/grpcpp.h>
#include "MetaInfo.h"
#include "proto/coordinator.grpc.pb.h"

namespace lrc{
    class FileSystemClient{

        //for validation
        std::string m_meta_path ;
        std::string m_conf_path ;

        //read from conf
        std::string m_fs_uri ; //  denotes coordinator
        std::string m_self_uri ;//denotes self
        std::string m_log_path ;// denotes log path

        int m_default_blk_size;

        // cache filesystem metadata for validation
        std::unordered_map<int,StripeInfo> stripe_info;

        // concurrent r/w stripe

        // client-namenode stub
        // no client-datanode stub, because client can see a filesystem abstraction via coordinator
        std::unique_ptr<coordinator::FileSystem> m_fileSystem_ptr;

        // need a socketfactory and a logger
    public:
        FileSystemClient(const std::string & p_conf_path,const std::string p_fsimage_path):m_conf_path(p_conf_path)
        {
            //parse config file
            pugi::xml_document doc;
            doc.load_file(p_conf_path.data(),pugi::parse_default,pugi::encoding_utf8);

            pugi::xml_node root_node = doc.child("root");
            pugi::xml_node properties_node = root_node.child("properties");

            for(auto property_node = properties_node.child("property");property_node;properties_node.next_sibling())
            {
                auto prop_name = std::string(property_node.child("name").value());
                auto prop_value = std::string(property_node.child("value").value());

                if("fs_uri" == prop_name)
                {
                    m_fs_uri = prop_value;
                }else if("log_path" == prop_name)
                {
                    m_log_path = prop_value;
                }else if("default_block_size" == prop_name)
                {
                    m_default_blk_size=std::stoi(prop_value);
                }
            }
            auto channel = grpc::CreateChannel(m_fs_uri,grpc::InsecureChannelCredentials());
            m_fileSystem_ptr=coordinator::FileSystem::NewStub(channel);
        }

        bool UploadStripe(const std::string & srcpath,const std::string & dstpath,const ECSchema & ecschema,int stripeid)
        {
            // request DNs Location
            //
        }
    };
}
#endif //LRC_FILESYSTEMCLIENT_H
