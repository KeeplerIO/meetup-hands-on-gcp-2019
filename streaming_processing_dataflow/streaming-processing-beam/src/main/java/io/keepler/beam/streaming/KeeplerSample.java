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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.avro.reflect.Nullable;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
     * Class to hold info about an App.
     */
    @DefaultCoder(AvroCoder.class)
    static class AppRecord {

        String app;
        @Nullable
        String category;
        @Nullable
        Float rating;
        @Nullable
        Integer reviews;
        @Nullable
        Float size;
        @Nullable
        Integer installs;
        @Nullable
        String type;
        @Nullable
        Float price;
        @Nullable
        String contentRating;
        @Nullable
        String genres;
        @Nullable
        Date lastUpdated;
        @Nullable
        String currentVersion;
        @Nullable
        String androidVersion;

        public AppRecord() {}

        public AppRecord(String app, String category, Float rating, Integer reviews, Float size,
                         Integer installs, String type, Float price, String content_rating,
                         String genres, Date last_updated, String current_version,
                         String android_version) {
            this.app = app;
            this.category = category;
            this.rating = rating;
            this.reviews = reviews;
            this.size = size;
            this.installs = installs;
            this.type = type;
            this.price = price;
            this.contentRating = content_rating;
            this.genres = genres;
            this.lastUpdated = last_updated;
            this.currentVersion = current_version;
            this.androidVersion = android_version;
        }

        public String getApp() {
            return app;
        }

        public String getCategory() {
            return category;
        }

        public Float getRating() {
            return rating;
        }

        public Integer getReviews() {
            return reviews;
        }

        public Float getSize() {
            return size;
        }

        public Integer getInstalls() {
            return installs;
        }

        public String getType() {
            return type;
        }

        public Float getPrice() {
            return price;
        }

        public String getContentRating() {
            return contentRating;
        }

        public String getGenres() {
            return genres;
        }

        public Date getLastUpdated() {
            return lastUpdated;
        }

        public String getCurrentVersion() {
            return currentVersion;
        }

        public String getAndroidVersion() {
            return androidVersion;
        }
    }

    static class ParseAppRecordFn extends DoFn<String, AppRecord> {

        private static final Logger LOG = LoggerFactory.getLogger(ParseAppRecordFn.class);
        //private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");

        private Float parseRating(String rating) {
            try {
                float ratingFloat = Float.parseFloat(rating);
                if (Float.isNaN(ratingFloat)) return null;
                return ratingFloat;
            } catch (NumberFormatException e) {
                return null;
            }
        }

        private Float parseSize(String size) {
            String sizeUpper = size.toUpperCase();
            int multiplier;
            switch (sizeUpper.charAt(sizeUpper.length()-1)) {
                case 'K':
                    multiplier = 1000;
                    break;
                case 'M':
                    multiplier = 1000 * 1000;
                    break;
                default:
                    multiplier = 1;
            }
            String sizeNumber = sizeUpper.replace("M", "").replace("K","");
            try {
                return (Float.parseFloat(sizeNumber) * multiplier) / 1000000; // size in MB
            } catch (NumberFormatException e) {
                return null;
            }
        }

        private Integer parseInstalls(String installs) {
            try {
                return Integer.parseInt(installs.replace("+", "").replace(",",""));
            } catch (NumberFormatException e) {
                return null;
            }
        }

        private Date parseLastUpdated(String lastUpdated) {
            try {
                return new SimpleDateFormat("MMMM d, yyyy").parse(lastUpdated);
            } catch (ParseException e) {
                return null;
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                CSVParser csvParser = CSVParser.parse(c.element(), CSVFormat.RFC4180.withHeader(HEADER.split(",")));
                for (CSVRecord csvRecord : csvParser) {
                    String app = csvRecord.get("App");
                    String category = csvRecord.get("Category");
                    Float rating = parseRating(csvRecord.get("Rating"));
                    Integer reviews = Integer.parseInt(csvRecord.get("Reviews"));
                    Float size = parseSize(csvRecord.get("Size"));
                    Integer installs = parseInstalls(csvRecord.get("Installs"));
                    String type = csvRecord.get("Type");
                    Float price = Float.parseFloat(csvRecord.get("Price").replace("$",""));
                    String contentRating = csvRecord.get("Content Rating");
                    String genres = csvRecord.get("Genres");
                    Date lastUpdated = parseLastUpdated(csvRecord.get("Last Updated"));
                    String currentVersion = csvRecord.get("Current Ver");
                    String androidVersion = csvRecord.get("Android Ver");
                    AppRecord appRecord = new AppRecord(app, category, rating, reviews, size, installs, type,
                            price, contentRating, genres, lastUpdated, currentVersion, androidVersion);
                    c.output(appRecord);
                }
            } catch (Exception e) {
                // numParseErrors.inc();
                LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
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

        String tableSpec = "meetup-hands-on-gcp-2019:googleplaystore_batch_dataflow.play_store_streaming";

        p
                .apply("ReadLines", PubsubIO.readStrings().fromTopic(options.getTopic()))
                .apply("Filter CSV Header", ParDo.of(new FilterCSVHeaderFn(HEADER)))
                .apply("ParseAppRecord", ParDo.of(new ParseAppRecordFn()))
                .apply(ParDo.of(new AppRecordToRowConverter()))
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

        runKeeplerSample(options);
    }
}
