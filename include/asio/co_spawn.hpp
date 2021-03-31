//
// co_spawn.hpp
// ~~~~~~~~~~~~
//
// Copyright (c) 2003-2020 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef ASIO_CO_SPAWN_HPP
#define ASIO_CO_SPAWN_HPP

#if defined(_MSC_VER) && (_MSC_VER >= 1200)
# pragma once
#endif // defined(_MSC_VER) && (_MSC_VER >= 1200)

#include "asio/detail/config.hpp"

#if defined(ASIO_HAS_CO_AWAIT) || defined(GENERATING_DOCUMENTATION)

#include "asio/awaitable.hpp"
#include "asio/execution/executor.hpp"
#include "asio/execution_context.hpp"
#include "asio/is_executor.hpp"

#include "asio/detail/push_options.hpp"

namespace asio {
namespace detail {

template <typename T>
struct awaitable_signature;

template <typename T, typename Executor>
struct awaitable_signature<awaitable<T, Executor>>
{
  typedef void type(std::exception_ptr, T);
};

template <typename Executor>
struct awaitable_signature<awaitable<void