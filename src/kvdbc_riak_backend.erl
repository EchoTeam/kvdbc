%%% vim: set ts=4 sts=4 sw=4 et:

-module(kvdbc_riak_backend).
-behaviour(kvdbc_backend).

-export([
    start_link/1,
    get/4,
    put/5,
    delete/4,
    list_buckets/2,
    list_keys/3
]).
-export_type([
    errors/0,
    error/0,
    process_name/0
]).

-define(HANDLER_MODULE, 'riakc_cluster').

-type errors() :: ?HANDLER_MODULE:errors().
-type error() :: ?HANDLER_MODULE:error().
-type process_name() :: ?HANDLER_MODULE:cluster_name().
-type table() :: ?HANDLER_MODULE:table().
-type key() :: ?HANDLER_MODULE:key().
-type value() :: ?HANDLER_MODULE:value().
-type opts() :: kvdbc:opts().
-type instance_name() :: kvdbc:instance_name().

start_link(InstanceName) ->
    ProcessName = kvdbc_cfg:config_val(InstanceName, process_name),
    Peers = kvdbc_cfg:config_val(InstanceName, peers),
    SName = kvdbc_cfg:config_val(InstanceName, server_sname),
    Port = kvdbc_cfg:config_val(InstanceName, server_port),
    Config = [
        {peers, [{list_to_atom(SName ++ "@" ++ H), {H, Port}} || H <- Peers]},
        {options, kvdbc_cfg:instance_val(InstanceName, config)}
    ],
    riakc_cluster:start_link(ProcessName, Config).

-spec put(instance_name(), table(), key(), value(), opts()) -> error() | 'ok'.
put(InstanceName, Table, Key, Value, _Opts) ->
    ProcessName = kvdbc_cfg:config_val(InstanceName, process_name),
    riakc_cluster:put(ProcessName, Table, Key, Value, [{w, 2}]).

-spec get(instance_name(), table(), key(), opts()) -> error() | {'ok', value()}.
get(InstanceName, Table, Key, _Opts) ->
    ProcessName = kvdbc_cfg:config_val(InstanceName, process_name),
    riakc_cluster:get(ProcessName, Table, Key, [{r, 2}]).

-spec delete(instance_name(), table(), key(), opts()) -> error() | 'ok'.
delete(InstanceName, Table, Key, _Opts) ->
    ProcessName = kvdbc_cfg:config_val(InstanceName, process_name),
    riakc_cluster:delete(ProcessName, Table, Key, [{rw, 2}]).

-spec list_keys(instance_name(), table(), opts()) -> error() | {'ok', [key()]}.
list_keys(InstanceName, Table, _Opts) ->
    ProcessName = kvdbc_cfg:config_val(InstanceName, process_name),
    riakc_cluster:list_keys(ProcessName, Table).

-spec list_buckets(instance_name(), opts()) -> error() | {'ok', [table()]}.
list_buckets(InstanceName, _Opts) ->
    ProcessName = kvdbc_cfg:config_val(InstanceName, process_name),
    riakc_cluster:list_buckets(ProcessName).
