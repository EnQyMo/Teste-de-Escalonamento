package utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;


public class JsonParser {
    private final ObjectMapper mapper;

    public JsonParser() {
        this.mapper = new ObjectMapper();
    }

    /**
     * Parse JSON file to object of specified type
     * @param filePath Path to the JSON file
     * @param valueType Class type to convert JSON to (can be single object or array)
     * @param <T> Generic type parameter
     * @return Parsed object of type T
     * @throws JsonParseException if there's an error parsing the JSON
     */
    public <T> T parse(String filePath, Class<T> valueType) throws JsonParseException {
        try {
            return mapper.readValue(new File(filePath), valueType);
        } catch (com.fasterxml.jackson.core.JsonParseException e) {
            throw new JsonParseException("Error parsing JSON file: " + e.getMessage(), e);
        } catch (com.fasterxml.jackson.databind.JsonMappingException e) {
            throw new JsonParseException("Error mapping JSON to object: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new JsonParseException("IO error reading JSON file: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new JsonParseException("Unexpected error parsing JSON: " + e.getMessage(), e);
        }
    }
}
