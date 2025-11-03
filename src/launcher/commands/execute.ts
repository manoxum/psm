// filename: src/launcher/commands/execute.ts

import {CommandModule} from "yargs";
import {execute, CustomOptions} from "../../tools/execute";


const command:CommandModule<CustomOptions, CustomOptions> = {
    command: "execute",
    describe: "Execute customs scripts into database",
    builder: args => {
        args.options( "schema", {
            type: "string",
            alias: "s"

        }).options( "label", {
            type: "string",
            alias: "l",

        }).options( "groups", {
            type: "string",
            alias: "g",
            array: true
        })

        return args;
    },
    handler:( argv) =>{
        execute(argv).then(value => {
            console.log("Operation finished with success!")
        }).catch( reason => {
            console.error( "Operation finished with error!");
            console.error( reason );
        })
    }
}

export = command;

