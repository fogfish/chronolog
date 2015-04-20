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
-module(chronolog).

-export([start/0]).
-export([
   new/1
  ,append/3
  ,stream/3
  ,mktag/3
  ,untag/3
  ,match/2
   %% @todo aika api
]).

%%
%% type definition
-type(fd()     :: any()).
-type(ticker() :: binary()).
-type(val()    :: integer() | uid:g()).
-type(tag()    :: binary()).
-type(series() :: [{tempus:t(), val()} | val()]).
-type(range()  :: {tempus:t(), tempus:t()} | integer()).


%%
%%
start() ->
   applib:boot(?MODULE, []).

%%
%% create new time-series cask (open existed)
%%   Options:
%%     {file, list()} - filename for time-series
%%     {chronon, tempus:t() | integer()} - time series chronon
-spec(new/1 :: (list()) -> {ok, fd()} | {error, any()}).

new(Opts) ->
   case supervisor:start_child(chronolog_cask_sup, [[{owner, self()}|Opts]]) of
      {ok, Pid} ->
         gen_server:call(Pid, i);
      Error     ->
         Error
   end.

%%
%% append value
-spec(append/3  :: (fd(), ticker(), series()) -> {ok, uid:l()}).

append(FD, Ticker, Series) ->
   {ok, Uid} = chronolog_file:ticker(FD, Ticker),
   lists:foreach(
      fun(X) -> 
         chronolog_file:append(FD, Uid, chronolog_file:encode(FD, X)) 
      end, 
      Series
   ),
   {ok, Uid}.

%%
%% read stream values
-spec(stream/3 :: (fd(), ticker(), range()) -> datum:stream()).

stream(FD, Ticker, {_, _}=Range) ->
   {ok, Uid} = chronolog_file:ticker(FD, Ticker),
   chronolog_file:stream(FD, Uid, Range);

stream(FD, Ticker, Sec) ->
   T = os:timestamp(),
   stream(FD, Ticker, {tempus:sub(T, Sec), T}).


%%
%% create ticker tags
-spec(mktag/3   :: (fd(), ticker(), tag()) -> ok).

mktag(FD, Ticker, Tag) ->
   {ok, Uid} = chronolog_file:ticker(FD, Ticker),
   chronolog_file:mktag(FD, Uid, Tag).

%%
%% remove ticker tags
-spec(untag/3   :: (fd(), ticker(), tag()) -> ok).

untag(FD, Ticker, Tag) ->
   {ok, Uid} = chronolog_file:ticker(FD, Ticker),
   chronolog_file:untag(FD, Uid, Tag).

%%
%% match all ticker to tag
-spec(match/2 :: (fd(), tag()) -> datum:stream()).

match(FD, Tag) ->
   chronolog_file:match(FD, Tag).



