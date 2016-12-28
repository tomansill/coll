# coll v0.4
A command-line program written in the D language, that detects and reports duplicate files within a directory. This program calculates SHA256 digest for every file in the directory and stores hash digest in the table to detect duplicate files with matching SHA256 digests found in the table. Then the program reports all of its file duplicates findings at the end of the program with their hash value.

# Compile:
Compile with any D lang compiler such as [dmd](https://wiki.dlang.org/DMD), [gdc](https://wiki.dlang.org/GDC), or [ldc](https://wiki.dlang.org/LDC).

Example:

    ldc coll.d

# Usage: 
    coll [-q] <directory> ...<additional_directories> [-n <directory> ...<additional_directories>]

-q - quiet mode flag, turns off the progress printing in the stdout (optional and default is OFF)

-n - exclude directories flag, the directories that is listed after the flag will be excluded from the search (optional)
