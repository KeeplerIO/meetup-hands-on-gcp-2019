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

package io.keepler.beam.streaming.model;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.util.Date;

@DefaultCoder(AvroCoder.class)
public class AppRecord {
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
