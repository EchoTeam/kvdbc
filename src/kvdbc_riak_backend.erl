%%% Copyright (c) 2012-2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: set ts=4 sts=4 sw=4 et:

-module(kvdbc_riak_backend).
-behaviour(kvdbc_backend).

-export([
    start_link/2,
    get/3,
    put/4,
    delete/3,
    list_buckets/1,
    list_keys/2
]).

start_link(ProcessName, BackendName) ->
    Config = kvdbc_cfg:backend_val(BackendName, config),
    riakc_cluster:start_link(ProcessName, Config).

put(ProcessName, Table, Key, Value) ->
    riakc_cluster:put(ProcessName, Table, Key, Value, [{w, 2}]).

get(ProcessName, Table, Key) -> 
    riakc_cluster:get(ProcessName, Table, Key, [{r, 2}]).

delete(ProcessName, Table, Key) ->
    riakc_cluster:delete(ProcessName, Table, Key, [{rw, 2}]).

list_keys(ProcessName, Table) ->
    riakc_cluster:list_keys(ProcessName, Table).

list_buckets(ProcessName) ->
    riakc_cluster:list_buckets(ProcessName).
