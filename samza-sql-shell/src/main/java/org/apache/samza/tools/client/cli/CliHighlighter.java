package org.apache.samza.tools.client.cli;

import org.apache.samza.tools.client.util.CliUtil;

import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.util.List;

// A primitive higlighter. Improve it together with completer.
// Redo with a different set ot jline API to get more control:
// Utilizing the parser/completer/highlighter together to avoid duplicate processing

// TODO A:
//  1. Handle commands containing spaces, like "SHOW TABLE"
//  2. Handle quotes " '
//  3. Handle escape sequences, like \' and \"
//  4. Color constant literals
//
// TODO B:
//  5. Handle table names, etc.

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
