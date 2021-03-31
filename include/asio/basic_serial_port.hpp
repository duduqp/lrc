//
// basic_serial_port.hpp
// ~~~~~~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2020 Christopher M. Kohlhoff (chris at kohlhoff dot com)
// Copyright (c) 2008 Rep Invariant Systems, Inc. (info@repinvariant.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef ASIO_BASIC_SERIAL_PORT_HPP
#define ASIO_BASIC_SERIAL_PORT_HPP

#if defined(_MSC_VER) && (_MSC_VER >= 1200)
# pragma once
#endif // defined(_MSC_VER) && (_MSC_VER >= 1200)

#include "asio/detail/config.hpp"

#if defined(ASIO_HAS_SERIAL_PORT) \
  || defined(GENERATING_DOCUMENTATION)

#include <string>
#include "asio/any_io_executor.hpp"
#include "asio/async_result.hpp"
#include "asio/detail/handler_type_requirements.hpp"
#include "asio/detail/io_object_impl.hpp"
#include "asio/detail/non_const_lvalue.hpp"
#include "asio/detail/throw_error.hpp"
#include "asio/detail/type_traits.hpp"
#include "asio/error.hpp"
#include "asio/execution_context.hpp"
#include "asio/serial_port_base.hpp"
#if defined(ASIO_HAS_IOCP)
# include "asio/detail/win_iocp_serial_port_service.hpp"
#else
# include "asio/detail/reactive_serial_port_service.hpp"
#endif

#if defined(ASIO_HAS_MOVE)
# include <utility>
#endif // defined(ASIO_HAS_MOVE)

#include "asio/detail/push_options.hpp"

namespace asio {

/// Provides serial port functionality.
/**
 * The basic_serial_port class provides a wrapper over serial port
 * functionality.
 *
 * @par Thread Safety
 * @e Distinct @e objects: Safe.@n
 * @e Shared @e objects: Unsafe.
 */
template <typename Executor = any_io_executor>
class basic_serial_port
  : public serial_port_base
{
public:
  /// The type of the executor associated with the object.
  typedef Executor executor_type;

  /// Rebinds the serial port type to another executor.
  template <typename Executor1>
  struct rebind_executor
  {
    /// The serial port type when rebound to the specified executor.
    typedef basic_serial_port<Executor1> other;
  };

  /// The native representation of a serial port.
#if defined(GENERATING_DOCUMENTATION)
  typedef implementation_defined native_handle_type;
#elif defined(ASIO_HAS_IOCP)
  typedef detail::win_iocp_serial_port_service::native_handle_type
    native_handle_type;
#else
  typedef detail::reactive_serial_port_service::native_handle_type
    native_handle_type;
#endif

  /// A basic_basic_serial_port is always the lowest layer.
  typedef basic_serial_port lowest_layer_type;

  /// Construct a basic_serial_port without opening it.
  /**
   * This constructor creates a serial port without opening it.
   *
   * @param ex The I/O executor that the serial port will use, by default, to
   * dispatch handlers for any asynchronous operations performed on the
   * serial port.
   */
  explicit basic_serial_port(const executor_type& ex)
    : impl_(ex)
  {
  }

  /// Construct a basic_serial_port without opening it.
  /**
   * This constructor creates a serial port without opening it.
   *
   * @param context An execution context which provides the I/O executor that
   * the serial port will use, by default, to dispatch handlers for any
   * asynchronous operations performed on the serial port.
   */
  template <typename ExecutionContext>
  explicit basic_serial_port(ExecutionContext& context,
      typename enable_if<
        is_convertible<ExecutionContext&, execution_context&>::value,
        basic_serial_port
      >::type* = 0)
    : impl_(context)
  {
  }

  /// Construct and open a basic_serial_port.
  /**
   * This constructor creates and opens a serial port for the specified device
   * name.
   *
   * @param ex The I/O executor that the serial port will use, by default, to
   * dispatch handlers for any asynchronous operations performed on the
   * serial port.
   *
   * @param device The platform-specific device name for this serial
   * port.
   */
  basic_serial_port(const executor_type& ex, const char* device)
    : impl_(ex)
  {
    asio::error_code ec;
    impl_.get_service().open(impl_.get_implementation(), device, ec);
    asio::detail::throw_error(ec, "open");
  }

  /// Construct and open a basic_serial_port.
  /**
   * This constructor creates and opens a serial port for the specified device
   * name.
   *
   * @param context An execution context which provides the I/O executor that
   * the serial port will use, by default, to dispatch handlers for any
   * asynchronous operations performed on the serial port.
   *
   * @param device The platform-specific device name for this serial
   * port.
   */
  template <typename ExecutionContext>
  basic_serial_port(ExecutionContext& context, const char* device,
      typename enable_if<
        is_convertible<ExecutionContext&, execution_context&>::value
      >::type* = 0)
    : impl_(context)
  {
    asio::error_code ec;
    impl_.get_service().open(impl_.get_implementation(), device, ec);
    asio::detail::throw_error(ec, "open");
  }

  /// Construct and open a basic_serial_port.
  /**
   * This constructor creates and opens a serial port for the specified device
   * name.
   *
   * @param ex The I/O executor that the serial port will use, by default, to
   * dispatch handlers for any asynchronous operations performed on the
   * serial port.
   *
   * @param device The platform-specific device name for this serial
   * port.
   */
  basic_serial_port(const executor_type& ex, const std::string& device)
    : impl_(ex)
  {
    asio::error_code ec;
    impl_.get_service().open(impl_.get_implementation(), device, ec);
    asio::detail::throw_error(ec, "open");
  }

  /// Construct and open a basic_serial_port.
  /**
   * This constructor creates and opens a serial port for the specified device
   * name.
   *
   * @param context An execution context which provides the I/O executor that
   * the serial port will use, by default, to dispatch handlers for any
   * asynchronous operations performed on the serial port.
   *
   * @param device The platform-specific device name for this serial
   * port.
   */
  template <typename ExecutionContext>
  basic_serial_port(ExecutionContext& context, const std::string& device,
      typename enable_if<
        is_convertible<ExecutionContext&, execution_context&>::value
      >::type* = 0)
    : impl_(context)
  {
    asio::error_code ec;
    impl_.get_service().open(impl_.get_implementation(), device, ec);
    asio::detail::throw_error(ec, "open");
  }

  /// Construct a basic_serial_port on an existing native serial port.
  /**
   * This constructor creates a serial port object to hold an existing native
   * serial port.
   *
   * @param ex The I/O executor that the serial port will use, by default, to
   * dispatch handlers for any asynchronous operations performed on the
   * serial port.
   *
   * @param native_serial_port A native serial port.
   *
   * @throws asio::system_error Thrown on failure.
   */
  basic_serial_port(const executor_type& ex,
      const native_handle_type& native_serial_port)
    : impl_(ex)
  {
    asio::error_code ec;
    impl_.get_service().assign(impl_.get_implementation(),
        native_serial_port, ec);
    asio::detail::throw_error(ec, "assign");
  }

  /// Construct a basic_serial_port on an existing native serial port.
  /**
   * This constructor creates a serial port object to hold an existing native
   * serial port.
   *
   * @param context An execution context which provides the I/O executor that
   * the serial port will use, by default, to dispatch handlers for any
   * asynchronous operations performed on the serial port.
   *
   * @param native_serial_port A native serial port.
   *
   * @throws asio::system_error Thrown on failure.
   */
  template <typename ExecutionContext>
  basic_serial_port(ExecutionContext& context,
      const native_handle_type& native_serial_port,
      typename enable_if<
        is_convertible<ExecutionContext&, execution_context&>::value
      >::type* = 0)
    : impl_(context)
  {
    asio::error_code ec;
    impl_.get_service().assign(impl_.get_implementation(),
        native_serial_port, ec);
    asio::detail::throw_error(ec, "assign");
  }

#if defined(ASIO_HAS_MOVE) || defined(GENERATING_DOCUMENTATION)
  /// Move-construct a basic_serial_port from another.
  /**
   * This constructor moves a serial port from one object to another.
   *
   * @param other The other basic_serial_port object from which the move will
   * occur.
   *
   * @note Following the move, the moved-from object is in the same state as if
   * constructed using the @c basic_serial_port(const executor_type&)
   * constructor.
   */
  basic_serial_port(basic_serial_port&& other)
    : impl_(std::move(other.impl_))
  {
  }

  /// Move-assign a basic_serial_port from another.
  /**
   * This assignment operator moves a serial port from one object to another.
   *
   * @param other The other basic_serial_port object from which the move will
   * occur.
   *
   * @note Following the move, the moved-from object is in the same state as if
   * constructed using the @c basic_serial_port(const executor_type&)
   * constructor.
   */
  basic_serial_port& operator=(basic_serial_port&& other)
  {
    impl_ = std::move(other.impl_);
    return *this;
  }
#endif // defined(ASIO_HAS_MOVE) || defined(GENERATING_DOCUMENTATION)

  /// Destroys the serial port.
  /**
   * This function destroys the serial port, cancelling any outstanding
   * asynchronous wait operations associated with the serial port as if by
   * calling @c cancel.
   */
  ~basic_serial_port()
  {
  }

  /// Get the executor associated with the object.
  executor_type get_executor() ASIO_NOEXCEPT
  {
    return impl_.get_executor();
  }

  /// Get a reference to the lowest layer.
  /**
   * This function returns a reference to the lowest layer in a stack of
   * layers. Since a basic_serial_port cannot contain any further layers, it
   * simply returns a reference to itself.
   *
   * @return A reference to the lowest layer in the stack of layers. Ownership
   * is not transferred to the caller.
   */
  lowest_layer_type& lowest_layer()
  {
    return *this;
  }

  /// Get a const reference to the lowest layer.
  /**
   * This function returns a const reference to the lowest layer in a stack of
   * layers. Since a basic_serial_port cannot contain any further layers, it
   * simply returns a reference to itself.
   *
   * @return A const reference to the lowest layer in the stack of layers.
   * Ownership is not transferred to the caller.
   */
  const lowest_layer_type& lowest_layer() const
  {
    return *this;
  }

  /// Open the serial port using the specified device name.
  /**
   * This function opens the serial port for the specified device name.
   *
   * @param device The platform-specific device name.
   *
   * @throws asio::system_error Thrown on failure.
   */
  void open(const std::string& device)
  {
    asio::error_code ec;
    impl_.get_service().open(impl_.get_implementation(), device, ec);
    asio::detail::throw_error(ec, "open");
  }

  /// Open the serial port using the specified device name.
  /**
   * This function opens the serial port using the given platform-specific
   * device name.
   *
   * @param device The platform-specific device name.
   *
   * @param ec Set the indicate what error occurred, if any.
   */
  ASIO_SYNC_OP_VOID open(const std::string& device,
      asio::error_code& ec)
  {
    impl_.get_service().open(impl_.get_implementation(), device, ec);
    ASIO_SYNC_OP_VOID_RETURN(ec);
  }

  /// Assign an existing native serial port to the serial port.
  /*
   * This function opens the serial port to hold an existing native serial port.
   *
   * @param native_serial_port A native serial port.
   *
   * @throws asio::system_error Thrown on failure.
   */
  void assign(const native_handle_type& native_serial_port)
  {
    asio::error_code ec;
    impl_.get_service().assign(impl_.get_implementation(),
        native_serial_port, ec);
    asio::detail::throw_error(ec, "assign");
  }

  /// Assign an existing native serial port to the serial port.
  /*
   * This function opens the serial port to hold an existing native serial port.
   *
   * @param native_serial_port A native serial port.
   *
   * @param ec Set to indicate what error occurred, if any.
   */
  ASIO_SYNC_OP_VOID assign(const native_handle_type& native_serial_port,
      asio::error_code& ec)
  {
    impl_.get_service().assign(impl_.get_implementation(),
        native_serial_port, ec);
    ASIO_SYNC_OP_VOID_RETURN(ec);
  }

  /// Determine whether the serial port is open.
  bool is_open() const
  {
    return impl_.get_service().is_open(impl_.get_implementation());
  }

  /// Close the serial port.
  /**
   * This function is used to close the serial port. Any asynchronous read or
   * write operations will be cancelled immediately, and will complete with the
   * asio::error::operation_aborted error.
   *
   * @throws asio::system_error Thrown on failure.
   */
  void close()
  {
    asio::error_code ec;
    impl_.get_service().close(impl_.get_implementation(), ec);
    asio::detail::throw_error(ec, "close");
  }

  /// Close the serial port.
  /**
   * This function is used to close the serial port. Any asynchronous read or
   * write operations will be cancelled immediately, and will complete with the
   * asio::error::operation_aborted error.
   *
   * @param ec Set to indicate what error occurred, if any.
   */
  ASIO_SYNC_OP_VOID close(asio::error_code& ec)
  {
    impl_.get_service().close(impl_.get_implementation(), ec);
    ASIO_SYNC_OP_VOID_RETURN(ec);
  }

  /// Get the native serial port representation.
  /**
   * This function may be used to obtain the underlying representation of the
   * serial port. This is intended to allow access to native serial port
   * func                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             