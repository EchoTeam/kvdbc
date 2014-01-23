%%% vim: ts=4 sts=4 sw=4 expandtab

-module(kvdbc_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, reload_config/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {module, kvdbc_cfg} = load_config(),
    kvdbc_sup:start_link().

stop(_State) ->
    ok.

reload_config() ->
    {module, kvdbc_cfg} = load_config(),
    ok.
    
load_config() ->
    Config = application:get_all_env(kvdbc),
    Spec = config_mod_spec(Config),
    mod_gen:go(Spec).

config_mod_spec(Config) ->
    Instances = proplists:get_value(backend_instances, Config),
    MetricsModule = proplists:get_value(metrics_module, Config),
    BVals = lists:umerge([
        [
            lists:flatten(io_lib:format("instance_val(~s, ~s) -> ~p", [I, K, V]))
            || {K, V} <- Opts
        ]
        || [{I, Opts}] <- Instances
    ]),
    CVals = lists:umerge([
        [
            lists:flatten(io_lib:format("config_val(~s, ~s) -> ~p", [I, C, W]))
            || {C, W} <- proplists:get_value(config, Opts) 
        ]
        || [{I, Opts}] <- Instances
    ]),
    ModSpec = [
        ["-module(kvdbc_cfg).\n"],
        ["-export([metrics_module/0, instances/0, instance_val/2, config_val/2]).\n"],
        % <BC>
        ["-export([backends/0, backend_val/2]).\n"],
        ["backends() -> instances().\n"],
        ["backend_val(InstanceName, Key) -> instance_val(InstanceName, Key).\n"],
        % </BC>
        ["metrics_module() -> ", io_lib:format("~p", [MetricsModule]), ".\n"],
        ["instances() -> ", io_lib:format("~p", [Instances]), ".\n"],
        [string:join(BVals, ";\n"), ".\n"],
        [string:join(CVals, ";\n"), "."]
    ],
    ModSpec.


%% ===================================================================
%% Tests
%% ===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

config_mod_spec_test_() ->
    [
        fun() ->
            Config = [
              {metrics_module, folsom_metrics},
              {backend_instances, [
                  [{instance1, [
                    {callback_module, module1},
                    {process_name, process1},
                    {config, [
                      {k1, v1}
                    ]}
                  ]}],
                  [{instance2, [
                    {callback_module, module2},
                    {process_name, process2},
                    {config, [
                      {k2, v2}
                    ]}
                  ]}]
                ]}
            ],
            Instances = proplists:get_value(backend_instances, Config),
            ResultSpec = config_mod_spec(Config),
            ExpectedSpec = [
                "-module(kvdbc_cfg).\n"
                "-export([metrics_module/0, instances/0, instance_val/2, config_val/2]).\n"
                "-export([backends/0, backend_val/2]).\n"
                "backends() -> instances().\n"
                "backend_val(InstanceName, Key) -> instance_val(InstanceName, Key).\n"
                "metrics_module() -> folsom_metrics.\n",
                "instances() -> ", io_lib:format("~p", [Instances]), ".\n",
                "instance_val(instance1, callback_module) -> module1;\n"
                "instance_val(instance1, process_name) -> process1;\n"
                "instance_val(instance1, config) -> [{k1,v1}];\n"
                "instance_val(instance2, callback_module) -> module2;\n"
                "instance_val(instance2, process_name) -> process2;\n"
                "instance_val(instance2, config) -> [{k2,v2}].\n"
                "config_val(instance1, k1) -> v1;\n"
                "config_val(instance2, k2) -> v2."
            ],

            ?debugFmt("ExpectedSpec:~n~s", [ExpectedSpec]),
            ?debugFmt("ResultSpec:~n~s", [ResultSpec]),

            ?assertEqual(iolist_to_binary(ExpectedSpec),
                iolist_to_binary(ResultSpec)),
            ?assertEqual({module, kvdbc_cfg},
                mod_gen:go(ResultSpec)),
            ?assertEqual(module1,
                kvdbc_cfg:instance_val(instance1, callback_module)),
            ?assertEqual(v2,
                kvdbc_cfg:config_val(instance2, k2))
        end
    ].

-endif.
