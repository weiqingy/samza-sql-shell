package org.apache.samza.tools.client.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CliUtil {
    private static final Logger LOG = LoggerFactory.getLogger(CliUtil.class);

    public static boolean isStringNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }

    // Trims: leading spaces; trailing spaces and ";"s
    public static String trimCommand(String command) {
        if(isStringNullOrEmpty(command))
            return command;

        int len = command.length();
        int st = 0;

        while ((st < len) && (command.charAt(st) <= ' ')) {
            st++;
        }
        while ((st < len) && ((command.charAt(len - 1) <= ' ')
                    || command.charAt(len - 1) == ';')) {
            len--;
        }
        return ((st > 0) || (len < command.length())) ? command.substring(st, len) : command;
    }

    public static List<String> splitWithSpace(String buffer) {
        List<String> list = new ArrayList<String>();
        if(isStringNullOrEmpty(buffer))
            return list;

        boolean prevIsSpace = Character.isSpaceChar(buffer.charAt(0));
        int prevPos = 0;
        for(int i = 1; i < buffer.length(); ++i) {
            char c = buffer.charAt(i);
            boolean isSpace = Character.isSpaceChar(c);
            if(isSpace != prevIsSpace) {
                list.add(buffer.substring(prevPos, i));
                prevPos = i;
                prevIsSpace = isSpace;
            }
        }
        list.add(buffer.substring(prevPos));
        return list;
    }

    public static String getFormattedValue(OutgoingMessageEnvelope envelope) {
        String value = new String((byte[]) envelope.getMessage());
        ObjectMapper mapper = new ObjectMapper();
        String formattedValue;
        try {
            Object json = mapper.readValue(value, Object.class);
            formattedValue = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
        } catch (IOException e) {
            formattedValue = value;
            LOG.error("Error while formatting json", e);
        }

        return formattedValue;
    }

    public static String getCompressedValue(OutgoingMessageEnvelope envelope) {
        return new String((byte[]) envelope.getMessage());
    }
}
