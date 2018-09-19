package org.apache.samza.tools.client.cli;

import org.apache.samza.tools.client.util.CliUtil;

import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.util.List;


public class CliHighlighter implements Highlighter {
    private static final List<String> keywords;
    static {
        keywords = CliCommandType.getAllCommands();
        keywords.add("FROM");
        keywords.add("WHERE");
    }


    //@Override
    public AttributedString highlight(LineReader reader, String buffer) {
        AttributedStringBuilder builder = new AttributedStringBuilder();
        List<String> tokens = CliUtil.splitWithSpace(buffer);

        for(String token : tokens) {
            if(isKeyword(token)) {
                builder.style(AttributedStyle.BOLD.foreground(AttributedStyle.YELLOW))
                        .append(token);
            } else {
                builder.style(AttributedStyle.DEFAULT)
                        .append(token);
            }
        }

        return builder.toAttributedString();
    }

    private boolean isKeyword(String token) {
        for(String keyword : keywords) {
            if(keyword.compareToIgnoreCase(token) == 0)
                return true;
        }
        return false;

    }
}
