%%
%%   Copyright (c) 2011 - 2015, Dmitry Kolesnikov
%%   All Rights Reserved.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
-module(chronolog_tests).
-include_lib("eunit/include/eunit.hrl").

%%%------------------------------------------------------------------
%%%
%%% suites
%%%
%%%------------------------------------------------------------------   

chronolog_usec_test_() ->
   {foreach,
      fun usec/0
     ,fun free/1
     ,[
         fun append/1,
         fun stream/1,
         fun mktag/1,
         fun match/1,
         fun untag/1 
      ]
   }.   


%%%------------------------------------------------------------------
%%%
%%% init
%%%
%%%------------------------------------------------------------------   

usec() ->
   chronolog:start(),
   {ok, FD} = chronolog:new([
      {file, "/tmp/chronolog/usec"}
   ]),
   FD.
   
free(FD) ->
   chronolog:free(FD),
   application:stop(chronolog),
   os:cmd("rm -Rf /tmp/chronolog").



%%%------------------------------------------------------------------
%%%
%%% unit tests
%%%
%%%------------------------------------------------------------------   
-define(N,   100).
-define(URN, <<"urn:chronolog:test">>).
-define(TAG, <<"test">>).

append(FD) ->
   Seq = lists:seq(0,?N),
   [?_assertMatch({ok, {uid, _}}, chronolog:append(FD, ?URN, [{os:timestamp(), X}])) || X <- Seq].

stream(FD) ->
   Seq = lists:seq(0,?N),
   [
      ?_assertMatch(Seq, unit(chronolog:stream(FD, ?URN, 60)))
   ].

mktag(FD) ->
   [?_assertMatch(ok, chronolog:mktag(FD, ?URN, ?TAG))].

match(FD) ->
   Seq = lists:seq(0,?N),
   [
      ?_assertMatch(Seq, 
         unit(chronolog:stream(FD, stream:head(chronolog:match(FD, ?TAG)), 60))
      )
   ].

untag(FD) ->
   [?_assertMatch(ok, chronolog:untag(FD, ?URN, ?TAG))].



%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   


%%
%% time series stream to list of values
unit(Stream) ->
   stream:list(stream:map(fun({_, X}) -> X end, Stream)).
