import {CommandModule} from "yargs";
import {generate} from "../../tools/generate";


const command:CommandModule = {
    command: "generate",
    describe: "Generate pre-migrate archives",
    builder: args => {
        return args;
    },
    handler: args => {
        try {
            generate()
        } catch ( e ) {
            console.error( e )
            console.log( e )
            throw e;
        }
    }
}

export = command;

