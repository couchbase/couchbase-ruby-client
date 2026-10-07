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

#include <algorithm>
#include <deque>
#include <iterator>
#include <memory>
#include <mutex>

#include <ruby.h>
#include <ruby/vm.h>

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

  // Returns the rb_protect state of the failed write, or zero. The caller jumps with it once its
  // own C++ objects are destroyed.
  int flush_deferred_messages()
  {
    int state = 0;
    {
      // The mutex is held only to move messages in and out of the queue. A longjmp out of the
      // Ruby logger would leave it locked, and a logger that releases the GVL would deadlock with
      // a flush that holds the GVL and waits for the mutex.
      std::deque<log_message_for_ruby> messages{};
      {
        std::lock_guard<Mutex> lock(spdlog::sinks::base_sink<Mutex>::mutex_);
        std::swap(messages, deferred_messages_);
      }
      // install_logger_shim may replace @__logger_shim while the logger runs, which drops the last
      // reference to it.
      VALUE logger = ruby_logger_;
      while (state == 0 && !messages.empty()) {
        state = write_message(logger, messages.front());
        // A message is retried once. An interrupt may land before its callback wrote it, and a
        // logger that fails on it every time must not stop the queue.
        if (state == 0 || messages.front().failed) {
          messages.pop_front();
        } else {
          messages.front().failed = true;
        }
      }
      RB_GC_GUARD(logger);
      // After a failed write the unwritten messages go back to the front of the queue, ahead of
      // those logged meanwhile. The caller's jump is not
      // delayed by writing them, and rb_errinfo() is left as the failing callback set it.
      if (!messages.empty()) {
        std::lock_guard<Mutex> lock(spdlog::sinks::base_sink<Mutex>::mutex_);
        std::move(
          deferred_messages_.begin(), deferred_messages_.end(), std::back_inserter(messages));
        std::swap(messages, deferred_messages_);
      }
    }
    return state;
  }

  // Moves the messages queued in other to the front of this sink's queue.
  void take_deferred_messages(ruby_logger_sink& other)
  {
    if (&other == this) {
      return;
    }
    std::scoped_lock lock(this->mutex_, other.mutex_);
    std::move(deferred_messages_.begin(),
              deferred_messages_.end(),
              std::back_inserter(other.deferred_messages_));
    std::swap(other.deferred_messages_, deferred_messages_);
    other.deferred_messages_.clear();
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
    bool failed{ false };
  };

  void sink_it_(const spdlog::details::log_msg& msg) override
  {
    deferred_messages_.emplace_back(log_message_for_ruby{
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
    auto nanoseconds =
      std::chrono::duration_cast<std::chrono::nanoseconds>(msg.time.time_since_epoch() - seconds);
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

  static VALUE invoke_log_rescue_standard_error(VALUE arg)
  {
    return rb_rescue(invoke_log, arg, nullptr, Qnil);
  }

  // Returns the rb_protect state, non-zero when the logger call ended by a jump other than a
  // rescued StandardError: another exception, a throw, or a thread kill.
  static int write_message(VALUE logger, const log_message_for_ruby& msg)
  {
    if (NIL_P(logger)) {
      return 0;
    }
    argument_pack args{ logger, msg };
    int state = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    rb_protect(invoke_log_rescue_standard_error, reinterpret_cast<VALUE>(&args), &state);
    return state;
  }

  VALUE ruby_logger_{ Qnil };
  std::deque<log_message_for_ruby> deferred_messages_{};
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
    core::logger::set_log_levels(core::logger::level::trace);
  } else if (type == rb_intern("debug")) {
    core::logger::set_log_levels(core::logger::level::debug);
  } else if (type == rb_intern("info")) {
    core::logger::set_log_levels(core::logger::level::info);
  } else if (type == rb_intern("warn")) {
    core::logger::set_log_levels(core::logger::level::warn);
  } else if (type == rb_intern("error")) {
    core::logger::set_log_levels(core::logger::level::err);
  } else if (type == rb_intern("critical")) {
    core::logger::set_log_levels(core::logger::level::critical);
  } else if (type == rb_intern("off")) {
    core::logger::set_log_levels(core::logger::level::off);
  } else {
    rb_raise(rb_eArgError, "Unsupported log level type: %+" PRIsVALUE, log_level);
    return Qnil;
  }
  return Qnil;
}

static VALUE
cb_Backend_get_log_level(VALUE /* self */)
{
  switch (core::logger::get_lowest_log_level()) {
    case core::logger::level::trace:
      return rb_id2sym(rb_intern("trace"));
    case core::logger::level::debug:
      return rb_id2sym(rb_intern("debug"));
    case core::logger::level::info:
      return rb_id2sym(rb_intern("info"));
    case core::logger::level::warn:
      return rb_id2sym(rb_intern("warn"));
    case core::logger::level::err:
      return rb_id2sym(rb_intern("error"));
    case core::logger::level::critical:
      return rb_id2sym(rb_intern("critical"));
    case core::logger::level::off:
      return rb_id2sym(rb_intern("off"));
  }
  return Qnil;
}

// Returns the rb_protect state of a failed flush, or zero. When sink is no longer the current
// one, its unwritten messages move to the front of the current sink's queue, because a replaced
// sink is never flushed again.
int
flush_sink(const ruby_logger_sink_ptr& sink)
{
  int state = sink->flush_deferred_messages();
  if (state != 0 && cb_global_sink && cb_global_sink != sink) {
    cb_global_sink->take_deferred_messages(*sink);
  }
  return state;
}

void
install_logger_sink(VALUE self, VALUE logger, VALUE log_level)
{
  cb_global_sink.reset();
  rb_iv_set(self, "@__logger_shim", logger);
  if (NIL_P(logger)) {
    return;
  }
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
    rb_iv_set(self, "@__logger_shim", Qnil);
    return;
  }

  auto sink = std::make_shared<ruby_logger_sink<std::mutex>>(logger);
  core::logger::configuration configuration;
  configuration.console = false;
  configuration.log_level = level;
  configuration.sink = sink;
  core::logger::create_file_logger(configuration);
  cb_global_sink = sink;
}

VALUE
cb_Backend_install_logger_shim(VALUE self, VALUE logger, VALUE log_level)
{
  if (!NIL_P(logger)) {
    Check_Type(log_level, T_SYMBOL);
  }
  VALUE old_logger = rb_iv_get(self, "@__logger_shim");
  // The new sink is installed before the old one is flushed, so a failed flush leaves the new
  // logger in place and hands it the old sink's unwritten messages. Without a new logger they
  // have no destination.
  auto old_sink = cb_global_sink;
  core::logger::reset();
  install_logger_sink(self, logger, log_level);
  int state = old_sink ? flush_sink(old_sink) : 0;
  RB_GC_GUARD(old_logger);
  if (state != 0) {
    throw ruby_jump(state);
  }
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

int
try_flush_logger()
{
  if (auto sink = cb_global_sink; sink) {
    return flush_sink(sink);
  }
  core::logger::flush();
  return 0;
}

void
flush_logger()
{
  if (int state = try_flush_logger(); state != 0) {
    rb_jump_tag(state);
  }
}

namespace
{
// Joins the async logger thread while it is still running. VM at-exit hooks
// run after every end proc and finalizer, so what those log, such as a cluster
// closed in its finalizer, still reaches the built-in sinks. Left to the spdlog
// destructors, the join happens after ExitProcess has killed the thread. That
// never returns under winpthreads, and the Ruby process hangs on Windows.
void
shutdown_logger(ruby_vm_t* /* vm */)
{
  core::logger::shutdown();
}
} // namespace

void
init_logger_methods(VALUE cBackend)
{
  // A failed flush leaves messages queued. At exit, flush again after a failure that is an
  // ordinary exception; a message is consumed after its second failure, so the loop ends. A signal,
  // exit, throw or thread kill stops the loop and is re-raised for the end-proc runner.
  rb_set_end_proc(
    [](VALUE) {
      // The swallowed exception must not replace the $! the exit started with, which the logger
      // sees on the next attempt. rb_set_errinfo accepts only nil or an Exception.
      VALUE exit_error = rb_errinfo();
      bool restorable = NIL_P(exit_error) || (RB_TYPE_P(exit_error, T_OBJECT) &&
                                              RTEST(rb_obj_is_kind_of(exit_error, rb_eException)));
      int state = 0;
      for (;;) {
        rb_protect(
          [](VALUE) -> VALUE {
            flush_logger();
            return Qnil;
          },
          Qnil,
          &state);
        if (state != 0) {
          VALUE error = rb_errinfo();
          if (!RB_TYPE_P(error, T_OBJECT) || RTEST(rb_obj_is_kind_of(error, rb_eSignal)) ||
              RTEST(rb_obj_is_kind_of(error, rb_eSystemExit))) {
            rb_jump_tag(state);
          }
        }
        if (restorable) {
          rb_set_errinfo(exit_error);
        }
        if (state == 0) {
          return;
        }
      }
    },
    Qnil);
  ruby_vm_at_exit(shutdown_logger);
  rb_define_singleton_method(cBackend, "set_log_level", cb_Backend_set_log_level, 1);
  rb_define_singleton_method(cBackend, "get_log_level", cb_Backend_get_log_level, 0);
  rb_define_singleton_method(
    cBackend, "install_logger_shim", cb_method<cb_Backend_install_logger_shim>::invoke, 2);
  rb_define_singleton_method(cBackend,
                             "enable_protocol_logger_to_save_network_traffic_to_file",
                             cb_Backend_enable_protocol_logger_to_save_network_traffic_to_file,
                             1);
}
} // namespace couchbase::ruby
