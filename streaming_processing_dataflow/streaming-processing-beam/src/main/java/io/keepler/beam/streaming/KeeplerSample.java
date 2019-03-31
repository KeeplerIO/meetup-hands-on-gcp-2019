/*
 * MIT License
 *
 * Copyright (c) 2019 Keepler
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package io.keepler.beam.streaming;

import io.keepler.beam.streaming.model.AppRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Validation.Required;

import java.text.SimpleDateFormat;

/**
 * An example that reads from Cloud Storage and writes in BigQuery.
 */
public class KeeplerSample {

    static final String HEADER = "App,Category,Rating,Reviews,Size,Installs,Type,Price,Content Rating,Genres,Last Updated,Current Ver,Android Ver";

    public interface KeeplerSampleOptions extends DataflowPipelineOptions {

        @Description("Cloud Pub/Sub topic to subscribe")
        @Required
        String getTopic();

        void setTopic(String value);

    }

    static class FilterCSVHeaderFn extends DoFn<String, String> {
        String headerFilter;

        public FilterCSVHeaderFn(String headerFilter) {
            this.headerFilter = headerFilter;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();
            // Filter out elements that match the HEADER
            if (!row.equals(this.headerFilter)) {
                c.output(row);
            }
        }
    }

    /**
     * Converts AppRecords into BigQuery rows.
     */
    static class AppRecordToRowConverter extends DoFn<AppRecord, TableRow> {
        private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(
                    new TableRow()
                            .set("app", c.element().getApp())
                            .set("category", c.element().getCategory())
                            .set("rating", c.element().getRating())
                            .set("reviews", c.element().getReviews())
                            .set("size", c.element().getSize())
                            .set("installs", c.element().getInstalls())
                            .set("type", c.element().getType())
                            .set("price", c.element().getPrice())
                            .set("content_rating", c.element().getContentRating())
                            .set("genres", c.element().getGenres())
                            .set("last_updated", df.format(c.element().getLastUpdated()))
                            .set("current_ver", c.element().getCurrentVersion())
                            .set("android_ver", c.element().getAndroidVersion())
            );
        }
    }

    static void runKeeplerSample(KeeplerSampleOptions options) {
        options.setStreaming(true);
        Pipeline p = Pipeline.create(options);

        String tableSpec = "meetup-hands-on-gcp-2019:googleplaystore_streaming_dataflow.play_store_streaming";

        p
                .apply("ReadLines", PubsubIO.readStrings().fromTopic(options.getTopic()))
                .apply("Filter CSV Header", ParDo.of(new FilterCSVHeaderFn(HEADER)))
                .apply("ParseAppRecord", ParDo.of(new ParseAppRecordFn(HEADER)))
                .apply("Map to BigQuery rows", ParDo.of(new AppRecordToRowConverter()))
                .apply("Write to BigQuery",
                        BigQueryIO.writeTableRows()
                                .to(tableSpec)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        KeeplerSampleOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(KeeplerSampleOptions.class);

        runKeeplerSample(options);
    }
}
