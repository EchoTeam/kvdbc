%%% vim: ts=4 sts=4 sw=4 expandtab

-module(kvdbc_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    kvdbc_sup:start_link().

stop(_State) ->
    ok.
