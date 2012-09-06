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


package com.twitter.zipkin.hadoop.sources

import com.twitter.zipkin.gen.{BinaryAnnotation, Span, Annotation}
import com.twitter.scalding._
import com.twitter.zipkin.gen
import scala.collection.JavaConverters._

/**
 * Preprocesses the data by merging different pieces of the same span
 */
class Preprocessed(args : Args) extends Job(args) with DefaultDateRangeJob {
  val preprocessed = SpanSource()
    .read
    .mapTo(0 ->('trace_id, 'id, 'parent_id, 'annotations, 'binary_annotations)) {
      s: Span => (s.trace_id, s.id, s.parent_id, s.annotations.toList, s.binary_annotations.toList)
    }
    .groupBy('trace_id, 'id, 'parent_id) {
      _.reduce('annotations, 'binary_annotations) {
        (left: (List[Annotation], List[BinaryAnnotation]), right: (List[Annotation], List[BinaryAnnotation])) =>
        (left._1 ++ right._1, left._2 ++ right._2)
      }
    }.write(Tsv("pps.tsv")) 
    
/* filter for trace_ids, span_ids */
  val onlyMerge = preprocessed
    .mapTo(('trace_id, 'id, 'parent_id, 'annotations, 'binary_annotations) -> 'span) {
    a : (Long, Long, Long, List[Annotation], List[BinaryAnnotation]) =>
      a match {
        case (tid, id, pid, annotations, binary_annotations) =>
          val span = new gen.Span(tid, "", id, annotations.asJava, binary_annotations.asJava)
          if (pid != 0) {
            span.setParent_id(pid)
          }
          span
      }
    }.write(PrepNoNamesSpanSource())   
}
