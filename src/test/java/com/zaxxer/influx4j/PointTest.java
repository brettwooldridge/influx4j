/*
 * Copyright (c) 2018, Brett Wooldridge.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.influx4j;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PointTest {
   private PointFactory pointFactory;

   @Before
   public void createFactory() throws Exception {
      pointFactory = PointFactory.builder().build();
   }

   @Test
   public void testRemoveOnlyTag() throws Exception {
      Point point = pointFactory.createPoint("testRemoveOnlyTag")
         .tag("fruit", "apple")
         .field("field", "foo")
         .timestamp();

      point = point.removeTag("fruit");

      Assert.assertEquals(tsString("testRemoveOnlyTag field=\"foo\"", point.getTimestamp()), point.toString());
   }

   @Test
   public void testRemoveFirstTag() throws Exception {
      Point point = pointFactory.createPoint("testRemoveFirstTag")
         .tag("zebra", "4")
         .tag("apple", "1")
         .tag("table", "3")
         .field("field", "foo")
         .timestamp();

      point = point.removeTag("zebra");

      Assert.assertEquals(tsString("testRemoveFirstTag,apple=1,table=3 field=\"foo\"", point.getTimestamp()), point.toString());
   }

   @Test
   public void testRemoveMiddleTag() throws Exception {
      Point point = pointFactory.createPoint("testRemoveMiddleTag")
         .tag("zebra", "4")
         .tag("apple", "1")
         .tag("table", "3")
         .field("field", "foo")
         .timestamp();

      point = point.removeTag("apple");

      Assert.assertEquals(tsString("testRemoveMiddleTag,table=3,zebra=4 field=\"foo\"", point.getTimestamp()), point.toString());
   }

   @Test
   public void testRemoveLastTag() throws Exception {
      Point point = pointFactory.createPoint("testRemoveLastTag")
         .tag("zebra", "4")
         .tag("apple", "1")
         .tag("table", "3")
         .field("field", "foo")
         .timestamp();

      point = point.removeTag("table");

      Assert.assertEquals(tsString("testRemoveLastTag,apple=1,zebra=4 field=\"foo\"", point.getTimestamp()), point.toString());
   }

   @Test
   public void testRemoveFirstTagThenAdd() throws Exception {
      Point point = pointFactory.createPoint("testRemoveFirstTagThenAdd")
         .tag("zebra", "4")
         .tag("apple", "1")
         .tag("table", "3")
         .field("field", "foo")
         .timestamp();

      point = point
         .removeTag("zebra")
         .tag("animal", "2");

      Assert.assertEquals(tsString("testRemoveFirstTagThenAdd,animal=2,apple=1,table=3 field=\"foo\"", point.getTimestamp()), point.toString());
   }

   @Test
   public void testRemoveMiddleTagThenAdd() throws Exception {
      Point point = pointFactory.createPoint("testRemoveMiddleTagThenAdd")
         .tag("zebra", "4")
         .tag("apple", "1")
         .tag("table", "3")
         .field("field", "foo")
         .timestamp();

      point = point
         .removeTag("apple")
         .tag("animal", "2");

      Assert.assertEquals(tsString("testRemoveMiddleTagThenAdd,animal=2,table=3,zebra=4 field=\"foo\"", point.getTimestamp()), point.toString());
   }

   @Test
   public void testRemoveLastTagThenAdd() throws Exception {
      Point point = pointFactory.createPoint("testRemoveLastTagThenAdd")
         .tag("zebra", "4")
         .tag("apple", "1")
         .tag("table", "3")
         .field("field", "foo")
         .timestamp();

      point = point
         .removeTag("table")
         .tag("animal", "2");

      Assert.assertEquals(tsString("testRemoveLastTagThenAdd,animal=2,apple=1,zebra=4 field=\"foo\"", point.getTimestamp()), point.toString());
   }

   @Test
   public void testEscapeFieldValue() throws Exception {
      // For field values:
      // - Backslash does not need to be escaped except if it comes last
      // - Double quote does always need to be escaped
      final Point point = pointFactory.createPoint("testEscapeLastCharacter")
         .tag("table", "3")
         .field("path", "D:\\documents\\節\\")
         .field("text", "TextWithDoubleQuotes(\")AtTheEnd\"")
         .timestamp();

      final String expectedPathAndText = String.format("path=\"%s\",text=\"%s\"",
            "D:\\documents\\節\\\\",
            "TextWithDoubleQuotes(\\\")AtTheEnd\\\""
      );

      Assert.assertEquals(tsString("testEscapeLastCharacter,table=3 " + expectedPathAndText, point.getTimestamp()), point.toString());
   }

   @Test
   public void testPreviousFieldsNotAccessibleAfterClose(){
      PointFactory customPointFactory = PointFactory.builder()
         .initialSize(1)
         .maximumSize(1)
         .build();
      final Point pointOld = customPointFactory.createPoint("testPreviousFieldsNotAccessibleAfterClose")
         .tag("testTag", "tag-value")
         .field("string-field1", "fieldValue")
         .field("string-field2", "fieldValue2")
         .field("long-field1", 4L)
         .field("long-field2", 8L)
         .field("double-field",15.16)
         .field("double-field2",23.42)
         .field("bool-field1",true)
         .field("bool-field2",true)
         .timestamp();

      pointOld.close();

      final Point point = customPointFactory.createPoint("testPreviousFieldsNotAccessibleAfterClose")
         .tag("testTag", "tag-value")
         .field("string-field", "newValue")
         .field("long-field", 314L)
         .field("double-field",15.16)
         .field("bool-field",false)
         .timestamp();

      // old points field names should not be accessible when the point is reused
      Assert.assertNull("`string-field2` name should be null",point.stringFieldName(1));
      Assert.assertNull("`long-field2` name should be null",point.longFieldName(1));
      Assert.assertNull("`double-field2` name should be null",point.doubleFieldName(1));
      Assert.assertNull("`bool-field2` name should be null",point.booleanFieldName(1));
   }

   private static String tsString(final String str, final long timestamp) {
      return str + " " + timestamp + "\n";
   }
}
