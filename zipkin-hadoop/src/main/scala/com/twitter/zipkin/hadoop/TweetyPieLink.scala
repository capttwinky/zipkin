/*
* Copyright 2012 Twitter Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/  

package com.twitter.zipkin.hadoop

import com.twitter.scalding._
import cascading.pipe.joiner._
import com.twitter.zipkin.gen.{SpanServiceName, BinaryAnnotation, Span, Annotation}
import com.twitter.zipkin.hadoop.sources.{PrepTsvSource, PreprocessedSpanSource, Util}

/**
* Find out how often callers of TweetyPie end up hitting the backends
*/

class TweetyPieLink(args: Args) extends Job(args) with DefaultDateRangeJob {

  //Grab span info for this chunk
  val spanInfo = PreprocessedSpanSource()
  .read
  	//.limit(10000)
    .filter(0) { s : SpanServiceName => s.isSetParent_id() }
    .mapTo(0 -> ('trace_id, 'parent_id, 'service))
      { s: SpanServiceName => (s.trace_id, s.parent_id, s.service_name ) }

  //Find all spans that called TP
  val spanInfoTP = spanInfo
      .filter('service){s: String => s.toLowerCase == "tweetypie"}  

    // TODO: account for possible differences between sent and received service names
  val idName = PrepTsvSource()
    .read
  
  /* Join with the original on parent ID to get the parent's service name */
  val spanInfoWithChildIsTweetyPie = spanInfoTP
    .joinWithSmaller('parent_id -> 'id_1, idName, joiner = new LeftJoin)   //[{5}:'trace_id', 'parent_id', 'service', 'id_1', 'name_1']

  //Go back to the original span list, and find backends that TweetyPie calls
  val spanInfoWithParentIsTweetyPie = spanInfo
    .joinWithSmaller('parent_id -> 'id_1, idName, joiner = new LeftJoin)
    .filter('name_1) {s: String => Option(s)  //Perform null check before filtering.
      .map { _.toLowerCase == "tweetypie" }
      .getOrElse(false)
    }   // Filter first?  Consider creating another val and moving above the join.
    .rename(('trace_id, 'parent_id, 'service) -> ('trace_id_2, 'parent_id_2, 'service_2))
    .project('trace_id_2, 'parent_id_2, 'service_2)

  //Join the 'called by' and 'called' services by span ID
  val tweetypieJoin = spanInfoWithChildIsTweetyPie
      .joinWithSmaller('trace_id -> 'trace_id_2, spanInfoWithParentIsTweetyPie, joiner = new OuterJoin)
      .project('trace_id, 'name_1, 'service_2)
      .groupBy('trace_id, 'name_1, 'service_2){_.size}

  /*  .project('trace_id, 'name_1, 'name_2, 'unicorn)
      .groupBy('trace_id){ _.sortBy('trace_id) }    
 */   .write(Tsv(args("output")))
}
