/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <core/cluster.hxx>
#include <core/logger/configuration.hxx>
#include <core/logger/logger.hxx>
#include <core/platform/terminate_handler.h>

#include <asio/io_context.hpp>

#include <spdlog/cfg/env.h>
#include <spdlog/common.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/spdlog.h>

#include <memory>
#include <queue>

#include <ruby.h>

#include "rcb_logger.hxx"
#include "rcb_utils.hxx"

namespace couchbase::ruby
{
namespace
{
template<typename Mutex>
class ruby_logger_sink : public spdlog::sinks::base_sink<Mutex>
{
public:
  explicit ruby_logger_sink(VALUE ruby_logger)
    : ruby_logger_{ ruby_logger }
  {
  }

  void flush_deferred_messages()
  {
    std::lock_guard<Mutex> lock(spdlog::sinks::base_sink<Mutex>::mutex_);
    auto messages_ = std::move(deferred_messages_);
    while (!messages_.empty()) {
      write_message(messages_.front());
      messages_.pop();
    }
  }

  static VALUE map_log_level(spdlog::level::level_enum level)
  {
    switch (level) {
      case spdlog::level::trace:
        return rb_id2sym(rb_intern("trace"));
      case spdlog::level::debug:
        return rb_id2sym(rb_intern("debug"));
      case spdlog::level::info:
        return rb_id2sym(rb_intern("info"));
      case spdlog::level::warn:
        return rb_id2sym(rb_intern("warn"));
      case spdlog::level::err:
        return rb_id2sym(rb_intern("error"));
      case spdlog::level::critical:
        return rb_id2sym(rb_intern("critical"));
      case spdlog::level::off:
        return rb_id2sym(rb_intern("off"));
      default:
        break;
    }
    return Qnil;
  }

protected:
  struct log_message_for_ruby {
    spdlog::level::level_enum level{ spdlog::level::level_enum::info };
    spdlog::log_clock::time_point time;
    std::size_t thread_id{};
    std::string payload;
    const char* filename{ "<file>" };
    int line{};
    const char* funcname{ "<func>" };
  };

  void sink_it_(const spdlog::details::log_msg& msg) override
  {
    deferred_messages_.emplace(log_message_for_ruby{
      msg.level,
      msg.time,
      msg.thread_id,
      { msg.payload.begin(), msg.payload.end() },
      msg.source.filename,
      msg.source.line,
      msg.source.funcname,
    });
  }

  void flush_() override
  {
    /* do nothing here, the flush will be initiated by the SDK */
  }

private:
  struct argument_pack {
    VALUE logger;
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    const log_message_for_ruby& msg;
  };

  static VALUE invoke_log(VALUE arg)
  {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    auto* args = reinterpret_cast<argument_pack*>(arg);
    const auto& msg = args->msg;

    VALUE filename = Qnil;
    if (msg.filename != nullptr) {
      filename = cb_str_new(msg.filename);
    }
    VALUE line = Qnil;
    if (msg.line > 0) {
      line = ULL2NUM(msg.line);
    }
    VALUE function_name = Qnil;
    if (msg.funcname != nullptr) {
      function_name = cb_str_new(msg.funcname);
    }
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(msg.time.time_since_epoch());
    auto nanoseconds = msg.time.time_since_epoch() - seconds;
    return rb_funcall(args->logger,
                      rb_intern("log"),
                      8,
                      map_log_level(msg.level),
                      ULL2NUM(msg.thread_id),
                      ULL2NUM(seconds.count()),
                      ULL2NUM(nanoseconds.count()),
                      cb_str_new(msg.payload),
                      filename,
                      line,
                      function_name);
  }

  void write_message(const log_message_for_ruby& msg)
  {
    if (NIL_P(ruby_logger_)) {
      return;
    }
    argument_pack args{ ruby_logger_, msg };
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    rb_rescue(invoke_log, reinterpret_cast<VALUE>(&args), nullptr, Qnil);
  }

  VALUE ruby_logger_{ Qnil };
  std::queue<log_message_for_ruby> deferred_messages_{};
};

using ruby_logger_sink_ptr = std::shared_ptr<ruby_logger_sink<std::mutex>>;

ruby_logger_sink_ptr cb_global_sink{ nullptr };

VALUE
cb_Backend_enable_protocol_logger_to_save_network_traffic_to_file(VALUE /* self */, VALUE path)
{
  Check_Type(path, T_STRING);
  core::logger::configuration configuration{};
  configuration.filename = cb_string_new(path);
  core::logger::create_protocol_logger(configuration);
  return Qnil;
}

VALUE
cb_Backend_set_log_level(VALUE /* self */, VALUE log_level)
{
  Check_Type(log_level, T_SYMBOL);
  if (ID type = rb_sym2id(log_level); type == rb_intern("trace")) {
    spdlog::set_level(spdlog::level::trace);
  } else if (type == rb_intern("debug")) {
    spdlog::set_level(spdlog::level::debug);
  } else if (type == rb_intern("info")) {
    spdlog::set_level(spdlog::level::info);
  } else if (type == rb_intern("warn")) {
    spdlog::set_level(spdlog::level::warn);
  } else if (type == rb_intern("error")) {
    spdlog::set_level(spdlog::level::err);
  } else if (type == rb_intern("critical")) {
    spdlog::set_level(spdlog::level::critical);
  } else if (type == rb_intern("off")) {
    spdlog::set_level(spdlog::level::off);
  } else {
    rb_raise(rb_eArgError, "Unsupported log level type: %+" PRIsVALUE, log_level);
    return Qnil;
  }
  return Qnil;
}

static VALUE
cb_Backend_get_log_level(VALUE /* self */)
{
  switch (spdlog::get_level()) {
    case spdlog::level::trace:
      return rb_id2sym(rb_intern("trace"));
    case spdlog::level::debug:
      return rb_id2sym(rb_intern("debug"));
    case spdlog::level::info:
      return rb_id2sym(rb_intern("info"));
    case spdlog::level::warn:
      return rb_id2sym(rb_intern("warn"));
    case spdlog::level::err:
      return rb_id2sym(rb_intern("error"));
    case spdlog::level::critical:
      return rb_id2sym(rb_intern("critical"));
    case spdlog::level::off:
      return rb_id2sym(rb_intern("off"));
    case spdlog::level::n_levels:
      return Qnil;
  }
  return Qnil;
}

VALUE
cb_Backend_install_logger_shim(VALUE self, VALUE logger, VALUE log_level)
{
  core::logger::reset();
  rb_iv_set(self, "@__logger_shim", logger);
  if (NIL_P(logger)) {
    return Qnil;
  }
  Check_Type(log_level, T_SYMBOL);
  core::logger::level level{ core::logger::level::off };
  if (ID type = rb_sym2id(log_level); type == rb_intern("trace")) {
    level = core::logger::level::trace;
  } else if (type == rb_intern("debug")) {
    level = core::logger::level::debug;
  } else if (type == rb_intern("info")) {
    level = core::logger::level::info;
  } else if (type == rb_intern("warn")) {
    level = core::logger::level::warn;
  } else if (type == rb_intern("error")) {
    level = core::logger::level::err;
  } else if (type == rb_intern("critical")) {
    level = core::logger::level::critical;
  } else {
    rb_iv_set(self, "__logger_shim", Qnil);
    return Qnil;
  }

  auto sink = std::make_shared<ruby_logger_sink<std::mutex>>(logger);
  core::logger::configuration configuration;
  configuration.console = false;
  configuration.log_level = level;
  configuration.sink = sink;
  core::logger::create_file_logger(configuration);
  cb_global_sink = sink;
  return Qnil;
}

} // namespace

void
install_terminate_handler()
{
  if (auto env_val =
        spdlog::details::os::getenv("COUCHBASE_BACKEND_DONT_INSTALL_TERMINATE_HANDLER");
      env_val.empty()) {
    core::platform::install_backtrace_terminate_handler();
  }
}

void
init_logger()
{
  if (auto env_val = spdlog::details::os::getenv("COUCHBASE_BACKEND_DONT_USE_BUILTIN_LOGGER");
      env_val.empty()) {
    auto default_log_level = core::logger::level::info;
    if (env_val = spdlog::details::os::getenv("COUCHBASE_BACKEND_LOG_LEVEL"); !env_val.empty()) {
      default_log_level = core::logger::level_from_str(env_val);
    }

    core::logger::configuration configuration{};
    if (env_val = spdlog::details::os::getenv("COUCHBASE_BACKEND_LOG_PATH"); !env_val.empty()) {
      configuration.filename = env_val;
      configuration.filename += fmt::format(".{}", spdlog::details::os::pid());
    }
    configuration.console =
      spdlog::details::os::getenv("COUCHBASE_BACKEND_DONT_WRITE_TO_STDERR").empty();
    configuration.log_level = default_log_level;
    core::logger::create_file_logger(configuration);
    core::logger::set_log_levels(default_log_level);
  }
}

void
flush_logger()
{
  if (cb_global_sink) {
    cb_global_sink->flush_deferred_messages();
  } else {
    core::logger::flush();
  }
}

void
init_logger_methods(VALUE cBackend)
{
  rb_define_singleton_method(cBackend, "set_log_level", cb_Backend_set_log_level, 1);
  rb_define_singleton_method(cBackend, "get_log_level", cb_Backend_get_log_level, 0);
  rb_define_singleton_method(cBackend, "install_logger_shim", cb_Backend_install_logger_shim, 2);
  rb_define_singleton_method(cBackend,
                             "enable_protocol_logger_to_save_network_traffic_to_file",
                             cb_Backend_enable_protocol_logger_to_save_network_traffic_to_file,
                             1);
}
} // namespace couchbase::ruby
