import std.stdio;
import std.digest.sha;
import std.file;
import std.string;
import std.format;
import std.path;
import core.stdc.stdlib;
import std.container;
import std.parallelism;
import std.exception;

struct FilePaths{
    DList!string* list;
    uint list_length;
    string digest;
    ulong size;
}

struct CollisionSet{
    uint member = 0;
    FilePaths*[string] set;
    bool[string] collisions;
}

shared long prevLen = 0;
shared bool quiet = false;

void main(string[] args){
    // Write a welcome message
    writeln("coll v0.3 - finds duplicate files in directories");

    // Check input for any errors
    if(args.length < 2) usage(args[0]);

    // Check if user has flagged for quiet mode
    if(args[1] == "-q") quiet = true;

    // Process the arguments and check them for errors
    DList!string* directories = new DList!string();
    uint pos = -1;
    uint directories_size = 0;
    for(uint i = quiet ? 2 : 1; i < args.length; i++){
        if(args[i] == "-n"){
            pos = i + 1;
            break;
        }
        if(!exists(args[i])) usage(args[0], format("ERROR: Directory '%s' does not exist", args[i]));
        if(!isDir(args[i])) usage(args[0], format("ERROR: Specified path '%s' is not a directory", args[i]));
        directories.insertBack(args[i]);
        directories_size++;
    }

    if(directories_size == 0) usage(args[0]);

    bool[string] exclude;
    for(uint i = pos; i < args.length; i++){
        if(!exists(args[i])) usage(args[0], format("ERROR: Directory '%s' does not exist", args[i]));
        if(!isDir(args[i])) usage(args[0], format("ERROR: Specified path '%s' is not a directory", args[i]));
        exclude[args[i]] = true;
    }

    // Start the collection
    CollisionSet* set = searchCollisions(directories, exclude);

    // Process the collection
    if(set.collisions.length == 0) writeln("No duplicates found");
    else{
        ulong total_space = 0;
        uint collision_count = 0;
        foreach(string collision; set.collisions.keys()){
            collision_count++;

            // Get the list of collided files
            DList!string* list = set.set[collision].list;

            // Compute the wasted space
            ulong size = set.set[collision].size;
            size *= set.set[collision].list_length - 1;
            total_space += size;

            // Print the collided hash
            writef("Hash #%d: %s space used: %s\n", collision_count, collision, normalizeBytes(size));

            // Iterate the files
            uint count = 1;
            while(!list.empty()){
                // Dequeue the list
                string filepath = list.front();
                list.removeFront();

                // Print the result
                writef("\t%d) '%s'\n", count, filepath);

                // Increment the count
                count++;
            }
            writeln();
        }
        // Print results and exit
        writef("%d files searched. %d duplicates found. %s of storage space used for those duplicates.\n", set.member, set.collisions.length, normalizeBytes(total_space));
        stdout.flush();
    }
}

string normalizeBytes(ulong size){
    if(size < 1000) return format("%d B", size); // Bytes
    if(size >= 1000 && size < 1000000) return format("%.1f KB", size/1000.0f); // Kilobytes
    if(size >= 1000000 && size < 1000000000) return format("%.1f MB", size/1000000.0f); // Megabytes
    if(size >= 1000000000 && size < 1000000000000) return format("%.1f GB", size/1000000000.0f); // Gigabytes
    if(size >= 1000000000000 && size < 1000000000000000) return format("%.1f TB", size/1000000000000.0f); // Terabytes
    return format("%.1f PB", size/1000000000000000.0f); // Petabytes
}

CollisionSet* searchCollisions(DList!string* directories, bool[string] exclude){

    // Create a collection object
    CollisionSet* set = new CollisionSet();

    // Create a queue object
    DList!string* queue = new DList!string();
    while(!directories.empty()){
        string directory = directories.front();
        directories.removeFront();
        if(!(directory in exclude)) queue.insertBack(directory);
    }

    // Create a task pool
    TaskPool tp = new TaskPool();

    // Run until the queue is empty
    while(!queue.empty()){

        // poll the queue
        string element = queue.front();
        queue.removeFront();

        try{
            // If the element is a file, put it in the task pool and a thread will
            // pick it up and process it
            if(isFile(element) && !isSymlink(element))
                tp.put(task!processFile(element, set));

            // If the element is a directory, add items in the directory to the queue
            else if(isDir(element))
                foreach(string entry; dirEntries(element, SpanMode.shallow, false))
                    if(!(entry in exclude)) queue.insertBack(entry);
        }catch(Exception e){
            if(!quiet) cleanProgress();
            stderr.writef("ERROR: file '%s' failed to open\n", element);
        }
    }

    // Wait until all tasks are finished
    tp.finish(true);

    // Clean up the verbose text
    if(!quiet) cleanProgress();

    return set;
}

void processFile(string filepath, CollisionSet* set){

    // Check the input
    if(filepath == null || set == null) return;

    // Open the file
    File* file = null;
    try file = new File(filepath, "r");
    catch(ErrnoException e){
        stderr.writef("ERROR: file '%s' failed to open\n", filepath);
        return;
    }

    // Verbose
    if(!quiet) progress(filepath, set);

    // Read the file and digest the contents
    SHA256 sha256;
    sha256.start();
    while(!file.eof())
        sha256.put(cast(ubyte[])file.readln());

    // Get the result
    string digest = format(toHexString(sha256.finish()));

    // Add the result to the set
    addDigest(filepath, digest, set, file.size);
}

void addDigest(string filepath, string digest, CollisionSet* set, ulong size){
    // Check parameters
    if(filepath == null || digest == null || set == null){
        stderr.writef("ERROR: addDigest - one of the parameters is null\n");
        return;
    }

    // Do a synchronized insert into the set
    synchronized{
        if(digest in set.set){
            set.set[digest].list.insertBack(filepath);
            set.set[digest].list_length++;
            set.collisions[digest] = true;
        }else{
            DList!string* list = new DList!string();
            list.insertBack(filepath);
            FilePaths* fp = new FilePaths(list, 1, digest, size);
            set.set[digest] = fp;
        }
        set.member++;
    }
}

void progress(string filepath, CollisionSet* set){
    synchronized{
        // Get the formatted string
        string print = format("processed: %d  duplicates: %d   %s", set.member, set.collisions.length, baseName(filepath));

        // Print the string
        write(print);

        // Since that the string is printed on a carriage-returned line, some
        // characters from previous print may remain on the line, this code
        // will clear them out
        int len = cast(int) (prevLen - print.length);
        prevLen = print.length;
        if(len > 0) for(int i = 0; i < len; i++) write(" ");

        // Reset the carriage to the beginning of the line for next prints
        write("\r");
        stdout.flush();
    }
}

void cleanProgress(){
    write("\r");
    for(int i = 0; i < prevLen; i++) write(" ");
    write("\r");
    stdout.flush();
    prevLen = 0;
}

void usage(string program_name){
    usage(program_name, "");
}

void usage(string program_name, string message){
    if(message.length != 0) stderr.writeln(message);
    stderr.writef("Usage: %s -q <directory> ...<additonal_directories> -n <excluded_directory> ...<excluded_directories>\n", program_name);
    stderr.writeln("  [options]");
    stderr.writeln("\t-q :\tquiet mode flag - default is FALSE");
    stderr.writeln("\t-n :\texclude flag - list directories you want to be excluded from the search");
    exit(-1);
}
