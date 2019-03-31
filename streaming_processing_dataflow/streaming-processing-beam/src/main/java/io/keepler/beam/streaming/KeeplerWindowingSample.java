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

import com.google.api.services.bigquery.model.TableRow;
import io.keepler.beam.streaming.model.AppRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;


/**
 * An example that reads from Cloud Storage and writes the average ratings for each category
 * in BigQuery, using a fixed-time window.
 */
public class KeeplerWindowingSample {

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
    static class AppRecordToRowConverter extends DoFn<KV<String, Double>, TableRow> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(
                    new TableRow()
                            .set("timestamp", Instant.now().toString())
                            .set("category", c.element().getKey())
                            .set("rating", c.element().getValue())
            );
        }
    }

    static class AppRecordToKV extends DoFn<AppRecord, KV<String, Float>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            AppRecord app = c.element();
            String key = app.getCategory();
            Float rating = app.getRating();
            if (key != null && rating != null) {
                c.output(KV.of(key, rating));
            }
        }
    }

    static void runKeeplerWindowingSample(KeeplerSampleOptions options) {
        options.setStreaming(true);
        Pipeline p = Pipeline.create(options);

        String tableSpec = "meetup-hands-on-gcp-2019:googleplaystore_streaming_dataflow.play_store_streaming_window";

        PCollection<AppRecord> apps = p
                .apply("ReadLines", PubsubIO.readStrings().fromTopic(options.getTopic()))
                .apply("Filter CSV Header", ParDo.of(new FilterCSVHeaderFn(HEADER)))
                .apply("ParseAppRecord", ParDo.of(new ParseAppRecordFn(HEADER)));

        PCollection<KV<String, Double>> avgRating = apps
                .apply("TimeWindow",
                        Window.into(FixedWindows.of(Duration.standardSeconds(30))))
                .apply("ByCategory", ParDo.of(new AppRecordToKV()))
                .apply("AvgByCategory", Mean.perKey());

        avgRating.apply(ParDo.of(new AppRecordToRowConverter()))
                .apply(
                        BigQueryIO.writeTableRows()
                                .to(tableSpec)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        KeeplerSampleOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(KeeplerSampleOptions.class);

        runKeeplerWindowingSample(options);
    }
}
