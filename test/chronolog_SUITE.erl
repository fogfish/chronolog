-module(chronolog_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([
   all/0,
   init_per_suite/1,
   end_per_suite/1
]).
-export([
   append_one_chunk/1,
   append_few_chunk/1,
   union/1,
   join/1,
   intersect/1,
   scan/1
]).

%%%----------------------------------------------------------------------------   
%%%
%%% Suite
%%%
%%%---------------------------------------------------------------------------- 
all() ->
   [Test || {Test, NAry} <- ?MODULE:module_info(exports), 
      Test =/= module_info,
      Test =/= init_per_suite,
      Test =/= end_per_suite,
      NAry =:= 1
   ].

init_per_suite(Config) ->
   chronolog:start(),
   Config.

end_per_suite(_Config) ->
   application:stop(chronolog).

%%%----------------------------------------------------------------------------   
%%%
%%% Units
%%%
%%%---------------------------------------------------------------------------- 
-define(A,   1543).
-define(B, 760175).

%%
append_one_chunk(_) ->
   Ticker  = append_one_chunk,
   {ok, _} = chronolog:new(Ticker, #{
      chronon => {0, 0, 100}
   ,  chunk   => 86400
   }),
   Expect = [{{?A, X, 0}, X} || X <- lists:seq(?B, ?B + 100)],
   chronolog:append(Ticker, Expect),
   Expect = stream:list(chronolog:stream(Ticker, {?A, ?B, 0}, {?A, ?B + 100, 0})).

%%
append_few_chunk(_) ->
   Ticker  = append_few_chunk,
   {ok, _} = chronolog:new(Ticker, #{
      chronon => {0, 0, 100}
   ,  chunk   => 10
   }),
   Expect = [{{?A, X, 0}, X} || X <- lists:seq(?B, ?B + 100)],
   chronolog:append(Ticker, Expect),
   Expect = stream:list(chronolog:stream(Ticker, {?A, ?B, 0}, {?A, ?B + 100, 0})).

%%
union(_) ->
   {ok, _} = chronolog:new(union_stream_1),
   {ok, _} = chronolog:new(union_stream_2),
   {ok, _} = chronolog:new(union_stream_3),
   Expect = [{{?A, X, 0}, X} || X <- lists:seq(?B, ?B + 100)],
   chronolog:append(union_stream_1, [{{?A, X, 0}, X} || X <- lists:seq(?B + 0, ?B + 100, 3)]),
   chronolog:append(union_stream_2, [{{?A, X, 0}, X} || X <- lists:seq(?B + 1, ?B + 100, 3)]),
   chronolog:append(union_stream_3, [{{?A, X, 0}, X} || X <- lists:seq(?B + 2, ?B + 100, 3)]),
   Expect = stream:list(
      chronolog:union(
         [union_stream_1, union_stream_2, union_stream_3], 
         {?A, ?B, 0}, {?A, ?B + 100, 0}
      )
   ).

%%
join(_) ->
   {ok, _} = chronolog:new(join_stream_1),
   {ok, _} = chronolog:new(join_stream_2),
   {ok, _} = chronolog:new(join_stream_3),
   Expect = [{{?A, X, 0}, [X, X, X]} || X <- lists:seq(?B, ?B + 100)],
   chronolog:append(join_stream_1, [{{?A, X, 0}, X} || X <- lists:seq(?B, ?B + 100)]),
   chronolog:append(join_stream_2, [{{?A, X, 0}, X} || X <- lists:seq(?B, ?B + 100)]),
   chronolog:append(join_stream_3, [{{?A, X, 0}, X} || X <- lists:seq(?B, ?B + 100)]),
   Expect = stream:list(
      chronolog:join(
         [join_stream_1, join_stream_2, join_stream_3], 
         {?A, ?B, 0}, {?A, ?B + 100, 0}
      )
   ).

%%
intersect(_) ->
   {ok, _} = chronolog:new(intersect_stream_1),
   {ok, _} = chronolog:new(intersect_stream_2),
   {ok, _} = chronolog:new(intersect_stream_3),
   Expect = [{{?A, X, 0}, [X, X, X]} || X <- lists:seq(?B + 3, ?B + 100)],
   chronolog:append(intersect_stream_1, [{{?A, X, 0}, X} || X <- lists:seq(?B + 0, ?B + 100)]),
   chronolog:append(intersect_stream_2, [{{?A, X, 0}, X} || X <- lists:seq(?B + 3, ?B + 100)]),
   chronolog:append(intersect_stream_3, [{{?A, X, 0}, X} || X <- lists:seq(?B + 3, ?B + 100)]),
   Expect = stream:list(
      chronolog:intersect(
         [intersect_stream_1, intersect_stream_2, intersect_stream_3], 
         {?A, ?B, 0}, {?A, ?B + 100, 0}
      )
   ).

%%
scan(_) ->
   {ok, _} = chronolog:new(scan_stream),
   Expect = [{{?A, X, 0}, X} || X <- lists:seq(?B, ?B + 100)],
   chronolog:append(scan_stream, Expect),
   Expect = stream:list(
      chronolog:scan(fun lists:max/1, 1,
         chronolog:stream(
            scan_stream,
            {?A, ?B, 0}, {?A, ?B + 100, 0}
         )
      )
   ).
