%%% Copyright (c) 2012-2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: set ts=4 sts=4 sw=4 et:

-module(kvdbc_riak_backend).
-behaviour(kvdbc_backend).

-export([
    start_link/1,
    get/3,
    put/4,
    delete/3,
    list_buckets/1,
    list_keys/2
]).

start_link(BackendName) ->
    ProcessName = kvdbc_cfg:val(BackendName, process_name),
    Config = kvdbc_cfg:val(BackendName, config),
    riakc_cluster:start_link(ProcessName, Config).

put(BackendName, Table, Key, Value) ->
    ProcessName = kvdbc_cfg:val(BackendName, process_name),
    riakc_cluster:put(ProcessName, Table, Key, Value, [{w, 2}]).

get(BackendName, Table, Key) -> 
    ProcessName = kvdbc_cfg:val(BackendName, process_name),
    riakc_cluster:get(ProcessName, Table, Key, [{r, 2}]).

delete(BackendName, Table, Key) ->
    ProcessName = kvdbc_cfg:val(BackendName, process_name),
    riakc_cluster:delete(ProcessName, Table, Key, [{rw, 2}]).

list_keys(BackendName, Table) ->
    ProcessName = kvdbc_cfg:val(BackendName, process_name),
    riakc_cluster:list_keys(ProcessName, Table).

list_buckets(BackendName) ->
    ProcessName = kvdbc_cfg:val(BackendName, process_name),
    riakc_cluster:list_buckets(ProcessName).
