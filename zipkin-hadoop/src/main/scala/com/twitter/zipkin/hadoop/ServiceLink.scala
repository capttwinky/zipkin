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
import cascading.pipe.Pipe
import com.twitter.zipkin.gen.{SpanServiceName, BinaryAnnotation, Span, Annotation}
import com.twitter.zipkin.hadoop.sources.{PrepTsvSource, PreprocessedSpanSource, Util}

/**
* Find out how often callers of TweetyPie end up hitting the backends
*/


class ServiceLink(args: Args) extends Job(args) with DefaultDateRangeJob {
  // TODO: account for possible differences between sent and received service names

	def spanInfoWithPropertyIsService(spanInfo:Pipe,property:Symbol,service_name:String):Pipe = {
	  spanInfo.filter(property){s: String => Option(s)
	    .map{_.toLowerCase == service_name.toLowerCase}
	    .getOrElse(false)
	  }
	}
	
	def childIsService(spanInfo:Pipe,service_name:String):Pipe = 
			spanInfoWithPropertyIsService(spanInfo, 'service, service_name)
			
	def parentIsService(spanInfo:Pipe,service_name:String):Pipe =
	  spanInfoWithPropertyIsService(spanInfo, 'parent_service, service_name)
	  .rename(('trace_id, 'service) -> ('trace_id_2, 'called_service))
	  .discard('parent_id, 'parent_service)
	  
  val idName = PrepTsvSource()
    .read
  //Grab span info for this chunk
  val spanInfo = PreprocessedSpanSource()
  .read
    //.filter(0) { s : SpanServiceName => s.isSetParent_id() }
    .mapTo(0 -> ('trace_id, 'parent_id, 'service))
      { s: SpanServiceName => (s.trace_id, s.parent_id, s.service_name ) }
    .joinWithSmaller('parent_id -> 'id_1, idName, joiner = new LeftJoin)
    .rename(('name_1) -> ('parent_service))
    .discard('id_1)
    
  /* Join with the original on parent ID to get the parent's service name */
  //Find all spans that called TP
  def makeServiceLink(service_name:String) =
  	{
    	val spanChild = childIsService(spanInfo, service_name)
    	//Go back to the original span list, and find backends that TweetyPie calls
    	val spanParent = parentIsService(spanInfo, service_name)
    	//Join the 'called by' and 'called' services by span ID
    	val serviceJoin = spanChild
	    	.joinWithSmaller('trace_id -> 'trace_id_2, spanParent, joiner = new OuterJoin)
	    	.project('parent_service, 'called_service)
	    	.groupBy('parent_service, 'called_service){_.size}
	    	.write(Tsv(args("output")+'/'+service_name))
  	}
  makeServiceLink("tweetypie")
  makeServiceLink("gizmoduck")
  makeServiceLink("timelineservice")
}
