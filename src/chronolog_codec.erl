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
-module(chronolog_codec).

-include("chronolog.hrl").

-export([
   t/1,
   seconds/1,
   encode/2
]).

%%
%% seconds to time stamp
t(Sec) ->
   {Sec div 1000000, Sec rem 1000000, 0}.

%%
%% time stamp to seconds
seconds({A, B, _}) ->
   A * 1000000 + B.

%%
%% encode list of value to time-series
encode(Chronolog, List) ->
   [encode_pair(Chronolog, Pair) || Pair <- List].

encode_pair(#chronolog{chronon = Ch}, {{_, _, _} = T, X}) ->
   {encode_t(Ch, T), X};
encode_pair(#chronolog{chronon = Ch}, X) ->
   {encode_t(Ch, os:timestamp()), X}.

encode_t({0, 0, 1}, T) ->
   T;
encode_t({_, _, _} = Ch, T) ->
   tempus:discrete(T, Ch).
