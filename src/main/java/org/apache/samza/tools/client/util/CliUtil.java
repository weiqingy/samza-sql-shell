package org.apache.samza.tools.client.util;

import java.util.ArrayList;
import java.util.List;

public class CliUtil {
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

}
