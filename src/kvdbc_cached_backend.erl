%%% Copyright (c) 2012-2013 JackNyfe. All rights reserved.
%%% THIS SOFTWARE IS PROPRIETARY AND CONFIDENTIAL. DO NOT REDISTRIBUTE.
%%%
%%% vim: set ts=4 sts=4 sw=4 et:

-module(kvdbc_cached_backend).
-behaviour(kvdbc_backend).

-export([
    start_link/2,
    get/4,
    put/5,
    delete/4,
    list_buckets/2,
    list_keys/3
]).

start_link(BackendName, ProcessName) ->
    Mod = module_name(BackendName),
    Mod:start_link(BackendName, ProcessName).

put(BackendName, ProcessName, Table, Key, Value) ->
    Mod = module_name(BackendName),
    Mod:put(BackendName, ProcessName, Table, Key, Value).

get(BackendName, ProcessName, Table, Key) -> 
    Mod = module_name(BackendName),
    Mod:get(BackendName, ProcessName, Table, Key).

delete(BackendName, ProcessName, Table, Key) ->
    Mod = module_name(BackendName),
    Mod:delete(BackendName, ProcessName, Table, Key).

list_keys(BackendName, ProcessName, Table) ->
    Mod = module_name(BackendName),
    Mod:list_keys(BackendName, ProcessName, Table).

list_buckets(BackendName, ProcessName) ->
    Mod = module_name(BackendName),
    Mod:list_buckets(BackendName, ProcessName).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

module_name(BackendName) ->
    Config = kvdbc_cfg:backend_val(BackendName, config),
    proplists:get_value(wrapped_backend_module, Config).
