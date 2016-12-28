/*  coll - A commandline program that detects duplicate files in directories
 *  @author Thomas Ansill
 *  @date 12/27/2016
 */
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

/** FilePaths structure
 *  contains a list of file paths that share the same digest
 */
struct FilePaths{
    DList!string* list; // list of file paths
    uint length; // length of file path list
    string digest; // digest
    ulong size; // size of that file
}

/** CollisionSet structure
 *  Maintains the set of CollisionSet
 */
struct CollisionSet{
    uint length = 0; // number of filepaths in the set
    FilePaths*[string] set; // the set of filepaths
    bool[string] collisions; // hashset of hash collisions
}

/** length of previous stdout write used in progress() function */
shared long prevLen = 0;

/** Quiet mode flag */
shared bool quiet = false;

/** Main method
 *  Starts the search and reports the results to stdout
 *  @param args Commandline arguments
 */
void main(string[] args){
    // Write a welcome message
    writeln("coll v0.4 - finds duplicate files in directories");

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

    // Check if input directories are not empty
    if(directories_size == 0) usage(args[0]);

    // Iterate the excluded directories list if -n is detected
    bool[string] exclude;
    if(pos != -1){
        for(uint i = pos; i < args.length; i++){
            if(!exists(args[i])) usage(args[0], format("ERROR: Directory '%s' does not exist", args[i]));
            if(!isDir(args[i])) usage(args[0], format("ERROR: Specified path '%s' is not a directory", args[i]));
            exclude[args[i]] = true;
        }
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
            size *= set.set[collision].length - 1;
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
        writef("%d files searched. %d duplicates found. %s of storage space used for those duplicates.\n", set.length, set.collisions.length, normalizeBytes(total_space));
        stdout.flush();
    }
}

/** Returns a string of space with a proper SI units
 *  @param size space in Bytes
 *  @return String that represents the space reduced to nearest tenths
 */
string normalizeBytes(ulong size){
    if(size < 1000) return format("%d B", size); // Bytes
    if(size >= 1000 && size < 1000000) return format("%.1f KB", size/1000.0f); // Kilobytes
    if(size >= 1000000 && size < 1000000000) return format("%.1f MB", size/1000000.0f); // Megabytes
    if(size >= 1000000000 && size < 1000000000000) return format("%.1f GB", size/1000000000.0f); // Gigabytes
    if(size >= 1000000000000 && size < 1000000000000000) return format("%.1f TB", size/1000000000000.0f); // Terabytes
    return format("%.1f PB", size/1000000000000000.0f); // Petabytes
}

/** Searches the directories for collisions
 *  @param directories List of directories to be searched
 *  @param exclude Set of directories to be excluded from the search
 *  @return CollisionSet object that contains all of the reported file duplicates if existed
 */
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
    tp.finish(true); // 'true' to block until all tasks finish

    // Clean up the verbose text
    if(!quiet) cleanProgress();

    // Done! Return the results
    return set;
}

/** Thread function - processes a file and add it to the table
 *  @param filepath filepath to be processed
 *  @param set Table of files for the file's digest to be added
 */
void processFile(string filepath, CollisionSet* set){

    // Check the input
    if(filepath == null || set == null) return;

    // Open the file - if file fails to open, just report error and move on
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

    // Close the file
    try file.close();
    catch(Exception e) return; // Do nothing on error
}

/** Synchronized function to insert the hash digest into the table
 *  @param filepath the path of file
 *  @param digest Digest of file
 *  @param set Table of files
 *  @param size Size of the file in Bytes
 */
void addDigest(string filepath, string digest, CollisionSet* set, ulong size){
    // Check parameters
    if(filepath == null || digest == null || set == null){
        stderr.writef("ERROR: addDigest - one of the parameters is null\n");
        return;
    }

    // Do a synchronized insert into the set
    synchronized{
        if(digest in set.set){ // Collision found
            set.set[digest].list.insertBack(filepath);
            set.set[digest].length++;
            set.collisions[digest] = true;
        }else{ // Collision not found
            DList!string* list = new DList!string();
            list.insertBack(filepath);
            FilePaths* fp = new FilePaths(list, 1, digest, size);
            set.set[digest] = fp;
        }
        set.length++;
    }
}

/** Verbose function to display the progress of directory search
 *  @param filepath Path of file currently being searched
 *  @param set Table of files
 */
void progress(string filepath, CollisionSet* set){
    synchronized{
        // Get the formatted string
        string print = format("processed: %d  duplicates: %d   %s", set.length, set.collisions.length, baseName(filepath));

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

/** Function to wipe the verbose output clean */
void cleanProgress(){
    write("\r");
    for(int i = 0; i < prevLen; i++) write(" ");
    write("\r");
    stdout.flush();
    prevLen = 0;
}

/** Reports program usage information without message
 *  @param program_name the name of program_name
 */
void usage(string program_name){
    usage(program_name, "");
}

/** Reports program usage information with message
 *  @param program_name the name of program_name
 *  @param message message to be displayed, if message is empty, then no message will be displayed
 */
void usage(string program_name, string message){
    if(message.length != 0) stderr.writeln(message);
    stderr.writef("Usage: %s -q <directory> ...<additonal_directories> -n <excluded_directory> ...<excluded_directories>\n", program_name);
    stderr.writeln("  [options]");
    stderr.writeln("\t-q :\tquiet mode flag - default is FALSE");
    stderr.writeln("\t-n :\texclude flag - list directories you want to be excluded from the search");
    exit(-1);
}
