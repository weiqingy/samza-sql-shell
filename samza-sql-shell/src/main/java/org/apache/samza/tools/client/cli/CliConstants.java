package org.apache.samza.tools.client.cli;


class CliConstants {
    public static final String APP_NAME = "Samza SQL Shell";
    public static final String WINDOW_TITLE = "Samza SQL Shell";
    public static final String PROMPT_1ST = "Samza SQL";
    public static final String PROMPT_1ST_END = "> ";
    public static final String PROMPT_2ND = "SQL";
    public static final String PROMPT_2ND_END = "> ";
    public static final String VERSION = "0.0.1";

    public static final String WELCOME_MESSAGE;
    static {

        WELCOME_MESSAGE =
"      ___           ___           ___           ___           ___ \n" +
"     /  /\\         /  /\\         /  /\\         /__/\\         /  /\\ \n" +
"    /  /::\\       /  /::\\       /  /::|        \\  \\:\\       /  /::\\ \n"+
"   /__/:/\\:\\     /  /:/\\:\\     /  /:|:|         \\  \\:\\     /  /:/\\:\\ \n"+
"  _\\_ \\:\\ \\:\\   /  /::\\ \\:\\   /  /:/|:|__        \\  \\:\\   /  /::\\ \\:\\ \n"+
" /__/\\ \\:\\ \\:\\ /__/:/\\:\\_\\:\\ /__/:/_|::::\\  ______\\__\\:\\ /__/:/\\:\\_\\:\\ \n"+
" \\  \\:\\ \\:\\_\\/ \\__\\/  \\:\\/:/ \\__\\/  /~~/:/ \\  \\::::::::/ \\__\\/  \\:\\/:/ \n"+
"  \\  \\:\\_\\:\\        \\__\\::/        /  /:/   \\  \\:\\~~~~~       \\__\\::/ \n"+
"   \\  \\:\\/:/        /  /:/        /  /:/     \\  \\:\\           /  /:/ \n"+
"    \\  \\::/        /__/:/        /__/:/       \\  \\:\\         /__/:/ \n"+
"     \\__\\/         \\__\\/         \\__\\/         \\__\\/         \\__\\/  \n\n"+
"Welcome to Samza SQL shell (V" + VERSION + "). Enter HELP for all commands.\n\n";
    }

    public static final char SPACE = '\u0020';


}
