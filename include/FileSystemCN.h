//
// Created by 杜清鹏 on 2021/3/29.
//

#ifndef LRC_FILESYSTEMCN_H
#define LRC_FILESYSTEMCN_H

#include "proto/coordinator.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "devcommon.h"

namespace lrc {
    class FileSystemCN final : public coordinator::FileSystem::Service {
    public:
        ::grpc::Status uploadStripe(::grpc::ServerContext *context, const ::coordinator::StripeInfo *request,
                                    ::coordinator::StripeLocation *response) override {
            return Service::uploadStripe(context, request, response);
        }

        ::grpc::Status createDir(::grpc::ServerContext *context, const ::coordinator::Path *request,
                                 ::coordinator::RequestResult *response) override {

            std::string dir = request->dstpath();

            if (checkdirexist(dir)) {
                response->set_trueorfalse(false);
            } else {
                appenddirectory(dir);
                response->set_trueorfalse(true);
            }

            return ::grpc::Status::OK;
        }

    private:
        bool checkdirexist(const std::string &path) const {
            // TO DO
            return 0 != m_fs_existeddir.count(path);
        }

        void appenddirectory(const std::string &path) {
            m_fs_existeddir.insert(path);
        }

        bool initialize() const {
            return true;
        }

        std::unordered_set<std::string> m_fs_existeddir;
    };
}


#endif //LRC_FILESYSTEMCN_H
