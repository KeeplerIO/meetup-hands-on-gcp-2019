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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ParseAppRecordFn extends DoFn<String, AppRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(ParseAppRecordFn.class);
    private String csvHeader;

    public ParseAppRecordFn(String csvHeader) {
        this.csvHeader = csvHeader;
    }

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
            CSVParser csvParser = CSVParser.parse(c.element(), CSVFormat.RFC4180.withHeader(csvHeader.split(",")));
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
            LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
        }
    }
}