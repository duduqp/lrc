// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: datanode.proto

#include "datanode.pb.h"
#include "datanode.grpc.pb.h"

#include <functional>
#include <grpcpp/impl/codegen/async_stream.h>
#include <grpcpp/impl/codegen/async_unary_call.h>
#include <grpcpp/impl/codegen/channel_interface.h>
#include <grpcpp/impl/codegen/client_unary_call.h>
#include <grpcpp/impl/codegen/client_callback.h>
#include <grpcpp/impl/codegen/message_allocator.h>
#include <grpcpp/impl/codegen/method_handler.h>
#include <grpcpp/impl/codegen/rpc_service_method.h>
#include <grpcpp/impl/codegen/server_callback.h>
#include <grpcpp/impl/codegen/server_callback_handlers.h>
#include <grpcpp/impl/codegen/server_context.h>
#include <grpcpp/impl/codegen/service_type.h>
#include <grpcpp/impl/codegen/sync_stream.h>
namespace datanode {

std::unique_ptr< FromDataNode::Stub> FromDataNode::NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options) {
  (void)options;
  std::unique_ptr< FromDataNode::Stub> stub(new FromDataNode::Stub(channel));
  return stub;
}

FromDataNode::Stub::Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel)
  : channel_(channel){}

FromDataNode::Service::Service() {
}

FromDataNode::Service::~Service() {
}


static const char* FromCoodinator_method_names[] = {
  "/datanode.FromCoodinator/handleupload",
  "/datanode.FromCoodinator/handledownload",
  "/datanode.FromCoodinator/clearallstripe",
  "/datanode.FromCoodinator/dolocallyrepair",
  "/datanode.FromCoodinator/docompleterepair",
  "/datanode.FromCoodinator/clearstripe",
  "/datanode.FromCoodinator/checkalive",
  "/datanode.FromCoodinator/handlepull",
  "/datanode.FromCoodinator/handlepush",
  "/datanode.FromCoodinator/pull_perform_push",
  "/datanode.FromCoodinator/renameblock",
};

std::unique_ptr< FromCoodinator::Stub> FromCoodinator::NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options) {
  (void)options;
  std::unique_ptr< FromCoodinator::Stub> stub(new FromCoodinator::Stub(channel));
  return stub;
}

FromCoodinator::Stub::Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel)
  : channel_(channel), rpcmethod_handleupload_(FromCoodinator_method_names[0], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_handledownload_(FromCoodinator_method_names[1], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_clearallstripe_(FromCoodinator_method_names[2], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_dolocallyrepair_(FromCoodinator_method_names[3], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_docompleterepair_(FromCoodinator_method_names[4], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_clearstripe_(FromCoodinator_method_names[5], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_checkalive_(FromCoodinator_method_names[6], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_handlepull_(FromCoodinator_method_names[7], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_handlepush_(FromCoodinator_method_names[8], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_pull_perform_push_(FromCoodinator_method_names[9], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_renameblock_(FromCoodinator_method_names[10], ::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  {}

::grpc::Status FromCoodinator::Stub::handleupload(::grpc::ClientContext* context, const ::datanode::UploadCMD& request, ::datanode::RequestResult* response) {
  return ::grpc::internal::BlockingUnaryCall(channel_.get(), rpcmethod_handleupload_, context, request, response);
}

void FromCoodinator::Stub::experimental_async::handleupload(::grpc::ClientContext* context, const ::datanode::UploadCMD* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_handleupload_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::handleupload(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_handleupload_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::handleupload(::grpc::ClientContext* context, const ::datanode::UploadCMD* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_handleupload_, context, request, response, reactor);
}

void FromCoodinator::Stub::experimental_async::handleupload(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_handleupload_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::AsynchandleuploadRaw(::grpc::ClientContext* context, const ::datanode::UploadCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_handleupload_, context, request, true);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::PrepareAsynchandleuploadRaw(::grpc::ClientContext* context, const ::datanode::UploadCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_handleupload_, context, request, false);
}

::grpc::Status FromCoodinator::Stub::handledownload(::grpc::ClientContext* context, const ::datanode::DownloadCMD& request, ::datanode::RequestResult* response) {
  return ::grpc::internal::BlockingUnaryCall(channel_.get(), rpcmethod_handledownload_, context, request, response);
}

void FromCoodinator::Stub::experimental_async::handledownload(::grpc::ClientContext* context, const ::datanode::DownloadCMD* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_handledownload_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::handledownload(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_handledownload_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::handledownload(::grpc::ClientContext* context, const ::datanode::DownloadCMD* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_handledownload_, context, request, response, reactor);
}

void FromCoodinator::Stub::experimental_async::handledownload(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_handledownload_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::AsynchandledownloadRaw(::grpc::ClientContext* context, const ::datanode::DownloadCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_handledownload_, context, request, true);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::PrepareAsynchandledownloadRaw(::grpc::ClientContext* context, const ::datanode::DownloadCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_handledownload_, context, request, false);
}

::grpc::Status FromCoodinator::Stub::clearallstripe(::grpc::ClientContext* context, const ::datanode::ClearallstripeCMD& request, ::datanode::RequestResult* response) {
  return ::grpc::internal::BlockingUnaryCall(channel_.get(), rpcmethod_clearallstripe_, context, request, response);
}

void FromCoodinator::Stub::experimental_async::clearallstripe(::grpc::ClientContext* context, const ::datanode::ClearallstripeCMD* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_clearallstripe_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::clearallstripe(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_clearallstripe_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::clearallstripe(::grpc::ClientContext* context, const ::datanode::ClearallstripeCMD* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_clearallstripe_, context, request, response, reactor);
}

void FromCoodinator::Stub::experimental_async::clearallstripe(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_clearallstripe_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::AsyncclearallstripeRaw(::grpc::ClientContext* context, const ::datanode::ClearallstripeCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_clearallstripe_, context, request, true);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::PrepareAsyncclearallstripeRaw(::grpc::ClientContext* context, const ::datanode::ClearallstripeCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_clearallstripe_, context, request, false);
}

::grpc::Status FromCoodinator::Stub::dolocallyrepair(::grpc::ClientContext* context, const ::datanode::NodesLocation& request, ::datanode::RequestResult* response) {
  return ::grpc::internal::BlockingUnaryCall(channel_.get(), rpcmethod_dolocallyrepair_, context, request, response);
}

void FromCoodinator::Stub::experimental_async::dolocallyrepair(::grpc::ClientContext* context, const ::datanode::NodesLocation* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_dolocallyrepair_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::dolocallyrepair(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_dolocallyrepair_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::dolocallyrepair(::grpc::ClientContext* context, const ::datanode::NodesLocation* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_dolocallyrepair_, context, request, response, reactor);
}

void FromCoodinator::Stub::experimental_async::dolocallyrepair(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_dolocallyrepair_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::AsyncdolocallyrepairRaw(::grpc::ClientContext* context, const ::datanode::NodesLocation& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_dolocallyrepair_, context, request, true);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::PrepareAsyncdolocallyrepairRaw(::grpc::ClientContext* context, const ::datanode::NodesLocation& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_dolocallyrepair_, context, request, false);
}

::grpc::Status FromCoodinator::Stub::docompleterepair(::grpc::ClientContext* context, const ::datanode::StripeLocation& request, ::datanode::RequestResult* response) {
  return ::grpc::internal::BlockingUnaryCall(channel_.get(), rpcmethod_docompleterepair_, context, request, response);
}

void FromCoodinator::Stub::experimental_async::docompleterepair(::grpc::ClientContext* context, const ::datanode::StripeLocation* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_docompleterepair_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::docompleterepair(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_docompleterepair_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::docompleterepair(::grpc::ClientContext* context, const ::datanode::StripeLocation* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_docompleterepair_, context, request, response, reactor);
}

void FromCoodinator::Stub::experimental_async::docompleterepair(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_docompleterepair_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::AsyncdocompleterepairRaw(::grpc::ClientContext* context, const ::datanode::StripeLocation& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_docompleterepair_, context, request, true);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::PrepareAsyncdocompleterepairRaw(::grpc::ClientContext* context, const ::datanode::StripeLocation& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_docompleterepair_, context, request, false);
}

::grpc::Status FromCoodinator::Stub::clearstripe(::grpc::ClientContext* context, const ::datanode::StripeId& request, ::datanode::RequestResult* response) {
  return ::grpc::internal::BlockingUnaryCall(channel_.get(), rpcmethod_clearstripe_, context, request, response);
}

void FromCoodinator::Stub::experimental_async::clearstripe(::grpc::ClientContext* context, const ::datanode::StripeId* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_clearstripe_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::clearstripe(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_clearstripe_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::clearstripe(::grpc::ClientContext* context, const ::datanode::StripeId* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_clearstripe_, context, request, response, reactor);
}

void FromCoodinator::Stub::experimental_async::clearstripe(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_clearstripe_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::AsyncclearstripeRaw(::grpc::ClientContext* context, const ::datanode::StripeId& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_clearstripe_, context, request, true);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::PrepareAsyncclearstripeRaw(::grpc::ClientContext* context, const ::datanode::StripeId& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_clearstripe_, context, request, false);
}

::grpc::Status FromCoodinator::Stub::checkalive(::grpc::ClientContext* context, const ::datanode::CheckaliveCMD& request, ::datanode::RequestResult* response) {
  return ::grpc::internal::BlockingUnaryCall(channel_.get(), rpcmethod_checkalive_, context, request, response);
}

void FromCoodinator::Stub::experimental_async::checkalive(::grpc::ClientContext* context, const ::datanode::CheckaliveCMD* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_checkalive_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::checkalive(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_checkalive_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::checkalive(::grpc::ClientContext* context, const ::datanode::CheckaliveCMD* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_checkalive_, context, request, response, reactor);
}

void FromCoodinator::Stub::experimental_async::checkalive(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_checkalive_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::AsynccheckaliveRaw(::grpc::ClientContext* context, const ::datanode::CheckaliveCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_checkalive_, context, request, true);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::PrepareAsynccheckaliveRaw(::grpc::ClientContext* context, const ::datanode::CheckaliveCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_checkalive_, context, request, false);
}

::grpc::Status FromCoodinator::Stub::handlepull(::grpc::ClientContext* context, const ::datanode::HandlePullCMD& request, ::datanode::RequestResult* response) {
  return ::grpc::internal::BlockingUnaryCall(channel_.get(), rpcmethod_handlepull_, context, request, response);
}

void FromCoodinator::Stub::experimental_async::handlepull(::grpc::ClientContext* context, const ::datanode::HandlePullCMD* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_handlepull_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::handlepull(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_handlepull_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::handlepull(::grpc::ClientContext* context, const ::datanode::HandlePullCMD* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_handlepull_, context, request, response, reactor);
}

void FromCoodinator::Stub::experimental_async::handlepull(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_handlepull_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::AsynchandlepullRaw(::grpc::ClientContext* context, const ::datanode::HandlePullCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_handlepull_, context, request, true);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::PrepareAsynchandlepullRaw(::grpc::ClientContext* context, const ::datanode::HandlePullCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_handlepull_, context, request, false);
}

::grpc::Status FromCoodinator::Stub::handlepush(::grpc::ClientContext* context, const ::datanode::HandlePushCMD& request, ::datanode::RequestResult* response) {
  return ::grpc::internal::BlockingUnaryCall(channel_.get(), rpcmethod_handlepush_, context, request, response);
}

void FromCoodinator::Stub::experimental_async::handlepush(::grpc::ClientContext* context, const ::datanode::HandlePushCMD* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_handlepush_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::handlepush(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_handlepush_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::handlepush(::grpc::ClientContext* context, const ::datanode::HandlePushCMD* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_handlepush_, context, request, response, reactor);
}

void FromCoodinator::Stub::experimental_async::handlepush(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_handlepush_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::AsynchandlepushRaw(::grpc::ClientContext* context, const ::datanode::HandlePushCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_handlepush_, context, request, true);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::PrepareAsynchandlepushRaw(::grpc::ClientContext* context, const ::datanode::HandlePushCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_handlepush_, context, request, false);
}

::grpc::Status FromCoodinator::Stub::pull_perform_push(::grpc::ClientContext* context, const ::datanode::OP& request, ::datanode::RequestResult* response) {
  return ::grpc::internal::BlockingUnaryCall(channel_.get(), rpcmethod_pull_perform_push_, context, request, response);
}

void FromCoodinator::Stub::experimental_async::pull_perform_push(::grpc::ClientContext* context, const ::datanode::OP* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_pull_perform_push_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::pull_perform_push(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_pull_perform_push_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::pull_perform_push(::grpc::ClientContext* context, const ::datanode::OP* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_pull_perform_push_, context, request, response, reactor);
}

void FromCoodinator::Stub::experimental_async::pull_perform_push(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_pull_perform_push_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::Asyncpull_perform_pushRaw(::grpc::ClientContext* context, const ::datanode::OP& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_pull_perform_push_, context, request, true);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::PrepareAsyncpull_perform_pushRaw(::grpc::ClientContext* context, const ::datanode::OP& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_pull_perform_push_, context, request, false);
}

::grpc::Status FromCoodinator::Stub::renameblock(::grpc::ClientContext* context, const ::datanode::RenameCMD& request, ::datanode::RequestResult* response) {
  return ::grpc::internal::BlockingUnaryCall(channel_.get(), rpcmethod_renameblock_, context, request, response);
}

void FromCoodinator::Stub::experimental_async::renameblock(::grpc::ClientContext* context, const ::datanode::RenameCMD* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_renameblock_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::renameblock(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc_impl::internal::CallbackUnaryCall(stub_->channel_.get(), stub_->rpcmethod_renameblock_, context, request, response, std::move(f));
}

void FromCoodinator::Stub::experimental_async::renameblock(::grpc::ClientContext* context, const ::datanode::RenameCMD* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_renameblock_, context, request, response, reactor);
}

void FromCoodinator::Stub::experimental_async::renameblock(::grpc::ClientContext* context, const ::grpc::ByteBuffer* request, ::datanode::RequestResult* response, ::grpc::experimental::ClientUnaryReactor* reactor) {
  ::grpc_impl::internal::ClientCallbackUnaryFactory::Create(stub_->channel_.get(), stub_->rpcmethod_renameblock_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::AsyncrenameblockRaw(::grpc::ClientContext* context, const ::datanode::RenameCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_renameblock_, context, request, true);
}

::grpc::ClientAsyncResponseReader< ::datanode::RequestResult>* FromCoodinator::Stub::PrepareAsyncrenameblockRaw(::grpc::ClientContext* context, const ::datanode::RenameCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc_impl::internal::ClientAsyncResponseReaderFactory< ::datanode::RequestResult>::Create(channel_.get(), cq, rpcmethod_renameblock_, context, request, false);
}

FromCoodinator::Service::Service() {
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FromCoodinator_method_names[0],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< FromCoodinator::Service, ::datanode::UploadCMD, ::datanode::RequestResult>(
          std::mem_fn(&FromCoodinator::Service::handleupload), this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FromCoodinator_method_names[1],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< FromCoodinator::Service, ::datanode::DownloadCMD, ::datanode::RequestResult>(
          std::mem_fn(&FromCoodinator::Service::handledownload), this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FromCoodinator_method_names[2],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< FromCoodinator::Service, ::datanode::ClearallstripeCMD, ::datanode::RequestResult>(
          std::mem_fn(&FromCoodinator::Service::clearallstripe), this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FromCoodinator_method_names[3],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< FromCoodinator::Service, ::datanode::NodesLocation, ::datanode::RequestResult>(
          std::mem_fn(&FromCoodinator::Service::dolocallyrepair), this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FromCoodinator_method_names[4],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< FromCoodinator::Service, ::datanode::StripeLocation, ::datanode::RequestResult>(
          std::mem_fn(&FromCoodinator::Service::docompleterepair), this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FromCoodinator_method_names[5],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< FromCoodinator::Service, ::datanode::StripeId, ::datanode::RequestResult>(
          std::mem_fn(&FromCoodinator::Service::clearstripe), this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FromCoodinator_method_names[6],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< FromCoodinator::Service, ::datanode::CheckaliveCMD, ::datanode::RequestResult>(
          std::mem_fn(&FromCoodinator::Service::checkalive), this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FromCoodinator_method_names[7],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< FromCoodinator::Service, ::datanode::HandlePullCMD, ::datanode::RequestResult>(
          std::mem_fn(&FromCoodinator::Service::handlepull), this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FromCoodinator_method_names[8],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< FromCoodinator::Service, ::datanode::HandlePushCMD, ::datanode::RequestResult>(
          std::mem_fn(&FromCoodinator::Service::handlepush), this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FromCoodinator_method_names[9],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< FromCoodinator::Service, ::datanode::OP, ::datanode::RequestResult>(
          std::mem_fn(&FromCoodinator::Service::pull_perform_push), this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      FromCoodinator_method_names[10],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< FromCoodinator::Service, ::datanode::RenameCMD, ::datanode::RequestResult>(
          std::mem_fn(&FromCoodinator::Service::renameblock), this)));
}

FromCoodinator::Service::~Service() {
}

::grpc::Status FromCoodinator::Service::handleupload(::grpc::ServerContext* context, const ::datanode::UploadCMD* request, ::datanode::RequestResult* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status FromCoodinator::Service::handledownload(::grpc::ServerContext* context, const ::datanode::DownloadCMD* request, ::datanode::RequestResult* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status FromCoodinator::Service::clearallstripe(::grpc::ServerContext* context, const ::datanode::ClearallstripeCMD* request, ::datanode::RequestResult* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status FromCoodinator::Service::dolocallyrepair(::grpc::ServerContext* context, const ::datanode::NodesLocation* request, ::datanode::RequestResult* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status FromCoodinator::Service::docompleterepair(::grpc::ServerContext* context, const ::datanode::StripeLocation* request, ::datanode::RequestResult* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status FromCoodinator::Service::clearstripe(::grpc::ServerContext* context, const ::datanode::StripeId* request, ::datanode::RequestResult* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status FromCoodinator::Service::checkalive(::grpc::ServerContext* context, const ::datanode::CheckaliveCMD* request, ::datanode::RequestResult* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status FromCoodinator::Service::handlepull(::grpc::ServerContext* context, const ::datanode::HandlePullCMD* request, ::datanode::RequestResult* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status FromCoodinator::Service::handlepush(::grpc::ServerContext* context, const ::datanode::HandlePushCMD* request, ::datanode::RequestResult* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status FromCoodinator::Service::pull_perform_push(::grpc::ServerContext* context, const ::datanode::OP* request, ::datanode::RequestResult* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status FromCoodinator::Service::renameblock(::grpc::ServerContext* context, const ::datanode::RenameCMD* request, ::datanode::RequestResult* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}


std::unique_ptr< FromClient::Stub> FromClient::NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options) {
  (void)options;
  std::unique_ptr< FromClient::Stub> stub(new FromClient::Stub(channel));
  return stub;
}

FromClient::Stub::Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel)
  : channel_(channel){}

FromClient::Service::Service() {
}

FromClient::Service::~Service() {
}


}  // namespace datanode

