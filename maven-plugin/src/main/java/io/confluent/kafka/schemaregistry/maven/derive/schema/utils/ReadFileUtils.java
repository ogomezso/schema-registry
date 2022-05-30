/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.maven.derive.schema.utils;

import static java.lang.Math.min;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to Read Files and generate messages in required format.
 */
public class ReadFileUtils {

  private static final Logger logger = LoggerFactory.getLogger(ReadFileUtils.class);

  static JSONParser jsonParser = new JSONParser();

  public static String readFile(String content) throws IOException {
    byte[] encoded = Files.readAllBytes(Paths.get(content));
    return new String(encoded, StandardCharsets.UTF_8);
  }

  /**
   * Given a file with array of JSONObjects, reads it and generates List of JSONObjects.
   * <p>
   * Eg, Expected File contents are :
   * [{"name":"J"}, {"name":"K"}, {"name":"L"}]
   * </p>
   *
   * @param content Content or name of the file to read
   * @return List of JSONObjects
   * @throws ParseException thrown when file not in correct format
   */
  public static List<Object> readArrayOfMessages(String content)
      throws ParseException {

    Object obj = jsonParser.parse(content);

    List<Object> listOfMessages = new ArrayList<>();
    for (Object x : (org.json.simple.JSONArray) obj) {
      listOfMessages.add(new JSONObject(x.toString()));
    }

    return listOfMessages;
  }

  /**
   * Given a file with JSONObjects which are line separated,
   * list of JSONObjects are generated and returned.
   * Eg, Expected File contents are :
   * <p>
   * {"name":"J"}
   * </p>
   * <p>
   * {"name":"K"}
   * </p>
   * <p>
   * {"name":"L"}
   * </p>
   * </p>
   *
   * @param content Name of the file to read
   * @return List of JSONObjects
   */
  public static List<Object> readLinesOfMessages(String content) {

    List<Object> listOfMessages = new ArrayList<>();
    String[] arrOfStr = content.split("\n");
    for (String line : arrOfStr) {
      listOfMessages.add(new JSONObject(line));
    }

    return listOfMessages;
  }


  private static void checkEmpty(String content) {
    if (content == null || content.length() == 0) {
      throw new IllegalArgumentException("Input file is empty.");
    }

  }

  private static List<Object> readCustom(String content) {

    checkEmpty(content);
    List<Object> ans = new ArrayList<>();

    try {
      ans.addAll(readArrayOfMessages(content));
      logger.info("Read input as array of Messages.");
      return ans;
    } catch (Exception ignored) {
      logger.info("Cannot be read input as array of Messages.");
    }

    try {
      ans.addAll(readLinesOfMessages(content));
      logger.info("Read input as lines of messages.");
      return ans;
    } catch (Exception ignored) {
      logger.info("Cannot be read input as lines of messages.");
    }

    try {
      ans.add(new JSONObject(content));
      logger.info("Read input as jsonObject.");
      return ans;
    } catch (Exception ignored) {
      logger.info("Cannot be read input as jsonObject.");
    }

    if (ans.isEmpty()) {
      logger.error("Unable to read messages.");
      throw new IllegalArgumentException("Input file format not understood.");
    }

    return ans;

  }


  /**
   * Reads string according to 3 formats, generating JSONObject for each message
   * and each message is converted to string and returned as a list.
   *
   * @param content string with messages
   * @return List of messages
   */
  public static List<String> readMessagesToString(String content) {

    logger.info(String.format("Reading input, Substring 0-20 is :%n  %s",
        content.substring(0, min(20, content.length()))));

    List<Object> listOfMessages = readCustom(content);
    List<String> listOfStrings = new ArrayList<>();
    for (Object m : listOfMessages) {
      listOfStrings.add(m.toString());
    }
    return listOfStrings;
  }

  /**
   * Reads file contents and generates string. The string is converted to list of messages.
   *
   * @param file input file to read
   * @return List of messages
   * @throws IOException thrown if unable to read ipnut file
   */
  public static List<String> readMessagesToString(File file) throws IOException {

    if (file == null) {
      throw new NullPointerException("Input file not set.");
    }
    logger.info(String.format("Reading input file %s", file.getName()));
    String fileContent = readFile(file.getAbsolutePath());
    return readMessagesToString(fileContent);

  }

}
