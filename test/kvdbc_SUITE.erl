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
            {process_name, riakc_default},
            {config, [
              {peers, [{'riak@localhost', {"localhost", 8087}}]}
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
        {backend_instances, [
          {cached, [
            {callback_module, kvdbc_cached_backend},
            {process_name, riakc_default},
            {config, [
              {cache_module, memcached_mock},
              {wrapped_backend_module, kvdbc_riak_backend},
              {peers, [{'riak@localhost', {"localhost", 8087}}]}
            ]}
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
    ok = kvdbc:put(<<"test_bucket">>, Key, Value),
    {ok, Value} = kvdbc:get(<<"test_bucket">>, Key),
    {ok, Value} = kvdbc:get(default, <<"test_bucket">>, Key),
    ok = kvdbc:delete(<<"test_bucket">>, Key),
    {error, notfound} = kvdbc:get(<<"test_bucket">>, Key),
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
    Key = list_to_binary(test_utils:unique_string()),
    Value = list_to_binary(test_utils:unique_string()),
    ok = kvdbc:put(cached, <<"test_bucket">>, Key, Value),
    {ok, Value} = kvdbc:get(cached, <<"test_bucket">>, Key),
    ok = kvdbc:delete(cached, <<"test_bucket">>, Key),
    {error, notfound} = kvdbc:get(cached, <<"test_bucket">>, Key),
    ok = application:stop(kvdbc),
    application:unload(kvdbc).
