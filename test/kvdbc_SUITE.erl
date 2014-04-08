%%% vim: ts=4 sts=4 sw=4 expandtab

-module(kvdbc_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0]).

-export([
    start_app_default_test/1,
    start_app_cached_test/1,
    backend_default_test/1,
    backend_cached_test/1
]).
 
all() -> [
    start_app_default_test,
    backend_default_test,
    start_app_cached_test,
    backend_cached_test
].

application_spec_default() ->
    {application, kvdbc,
     [
      {applications, [kernel, stdlib]},
      {mod, { kvdbc_app, []}},
      {env, [
        {backend_instances, [
          {default, [
            {callback_module, kvdbc_riak_backend},
            {config, [
              {process_name, riakc_default},
              {concurrency_level, 5},
              {peers, ["localhost"]},
              {server_sname, "riak"},
              {server_port, 8087}
            ]}
          ]}
        ]}
      ]}
     ]}.

application_spec_cached() ->
    {application, kvdbc,
     [
      {applications, [kernel, stdlib]},
      {mod, { kvdbc_app, []}},
      {env, [
        {metrics_module, metrics_mock},
        {backend_instances, [
          {riak, [
            {callback_module, kvdbc_riak_backend},
            {config, [
              {process_name, riakc_default},
              {concurrency_level, 5},
              {peers, ["localhost"]},
              {server_sname, "riak"},
              {server_port, 8087}
            ]}
          ]},
          {cached, [
            {callback_module, kvdbc_cached_backend},
            {config, [
              {cache_serverref, mb_riak_cache},
              {cache_module, memcached_mock},
              {backend_instance, riak}]}
          ]}
        ]}
      ]}
     ]}.
 
start_app_default_test(_Config) ->
    application:load(application_spec_default()),
    ok = application:start(kvdbc),
    true = whereis(kvdbc_sup) =/= undefined,
    true = whereis(riakc_default) =/= undefined,
    ok = application:stop(kvdbc),
    application:unload(kvdbc).

backend_default_test(_Config) ->
    application:load(application_spec_default()),
    ok = application:start(kvdbc),
    Key = list_to_binary(test_utils:unique_string()),
    Value = list_to_binary(test_utils:unique_string()),
    ok = kvdbc:put(default, <<"test_bucket">>, Key, Value),
    {ok, Value} = kvdbc:get(default, <<"test_bucket">>, Key),
    ok = kvdbc:delete(default, <<"test_bucket">>, Key),
    {error, notfound} = kvdbc:get(default, <<"test_bucket">>, Key),
    ok = application:stop(kvdbc),
    application:unload(kvdbc).
 
start_app_cached_test(_Config) ->
    application:load(application_spec_cached()),
    ok = application:start(kvdbc),
    true = whereis(kvdbc_sup) =/= undefined,
    true = whereis(riakc_default) =/= undefined,
    ok = application:stop(kvdbc),
    application:unload(kvdbc).

backend_cached_test(_Config) ->
    application:load(application_spec_cached()),
    ok = application:start(kvdbc),

    Counter = fun(Name) ->
        "kvdbc.kvdbc_cached_backend.cached." ++ Name
    end,

    0 = metrics_mock:get_metric(Counter("get")),
    0 = metrics_mock:get_metric(Counter("get_cached")),
    0 = metrics_mock:get_metric(Counter("put")),
    0 = metrics_mock:get_metric(Counter("delete")),

    Key = list_to_binary(test_utils:unique_string()),
    Value = list_to_binary(test_utils:unique_string()),

    ok = kvdbc:put(cached, <<"test_bucket">>, Key, Value),
    1 = metrics_mock:get_metric(Counter("put")),

    {ok, Value} = kvdbc:get(cached, <<"test_bucket">>, Key),
    0 = metrics_mock:get_metric(Counter("get")),
    1 = metrics_mock:get_metric(Counter("get_cached")),

    {ok, Value} = kvdbc:get(cached, <<"test_bucket">>, Key),
    0 = metrics_mock:get_metric(Counter("get")),
    2 = metrics_mock:get_metric(Counter("get_cached")),

    ok = kvdbc:delete(cached, <<"test_bucket">>, Key),
    1 = metrics_mock:get_metric(Counter("delete")),

    {error, notfound} = kvdbc:get(cached, <<"test_bucket">>, Key),
    1 = metrics_mock:get_metric(Counter("get")),

    ok = application:stop(kvdbc),
    application:unload(kvdbc).
