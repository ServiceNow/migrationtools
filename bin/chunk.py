#!/usr/bin/env python3

import os
import sys
import datetime
import argparse
import json
from hashlib import md5

def main():

    currentYear = datetime.datetime.now().date().strftime("%Y")

    parser = argparse.ArgumentParser(
        prog = 'chunk.py',
        description = 'Utility to split up large files into smaller files for parallel transfers',
        epilog = "This software is copyright " + currentYear + ", ServiceNow")

    parser.add_argument('-i', '--inputfile', help="The file to break apart (required)", required=True)
    parser.add_argument('-s', '--chunksize', help="Maximum size of each chunk, in MB. Defaults to 100MB. WARNING: This should not be set to more than 80 percent of the system\'s available memory", required=False)
    parser.add_argument('-b', '--block', help="Generate/recreate a specific block. Requires a start and end block. Using this option will not output a manifest file and the -s/--chunksize flag will be ignored. Enter the BLOCK value as a hyphen-separated string (eg. 12345-678901)", required=False)
    parser.add_argument('-v', '--verbose', help="Add verbosity", action="store_true")

    args        = parser.parse_args()

    inputFile   = args.inputfile
    verbose     = args.verbose
    chunkSize   = args.chunksize
    blockString = args.block
    data        = {}

    if chunkSize is None:
        chunkSize = 100
    else:
        chunkSize = int(chunkSize)

    chunkSize = chunkSize * 1048576  #Convert to Bytes
  
    try:
        fileInfo = os.stat(inputFile)

        if verbose == True:
            print("Processing input file: " + inputFile) 
        data['fileMode']          = oct(fileInfo.st_mode)[2:]
        data['fileSizeBytes']     = fileInfo.st_size
        data['fileName']          = os.path.basename(inputFile)
        data['filePath']          = os.path.dirname(inputFile)
        data['chunkMaxSizeBytes'] = chunkSize
        data['numChunks']         = 0
        data['chunkData']         = {}
    except:
        print("There was an issue reading \"" + inputFile + "\"! Quitting!", file=sys.stderr)
        raise
    
    if data['filePath'] == "":
        data['filePath'] = "."

    outputDir       = data['filePath'] + "/chunk"
    manifestFile    = "manifest.json"

    try:
        os.mkdir(outputDir)
        manifestFileOutput = open(outputDir + "/" + manifestFile, 'w')
    except:
        print("There was an issue setting up the output directory. Quitting!", file=sys.stderr)
        raise

    try:
        # Iterate over the input file, reading in 'chunkMaxSizeBytes' worth of data, running
        # an md5 hash against that chunk, and writing the data and md5 results out to files.
        # We'll also update an md5 hash against the full file for recording in the JSON at
        # the end, rather than calculating it all in one read.
        with open(inputFile, "rb") as f:
            full_file_hash = md5()
            fileCounter = 0
            bytePositionStart = 0
            bytePositionEnd   = 0
            if blockString is not None:
                (startBlock, endBlock) = str.split(blockString, '-')
                startBlock = int(startBlock)
                endBlock = int(endBlock)
                blocks_to_extract = endBlock - startBlock
                f.seek(startBlock)
                chunk = f.read(blocks_to_extract)
                file_hash = md5(chunk)
                chunkFileName = "blockExtract_" + file_hash.hexdigest() + ".part"
                of = open(outputDir + '/' + chunkFileName, "wb")
                of.write(chunk)
                of.close
            else:
                while bytePositionEnd < data['fileSizeBytes']:
                    bytePositionStart = 0 if (fileCounter == 0) else f.tell()
                    chunk = f.read(data['chunkMaxSizeBytes'])

                    full_file_hash.update(chunk)
                    file_hash = md5(chunk)

                    data['chunkData'][fileCounter]                      = {}
                    data['chunkData'][fileCounter]['position']          = '{:05}'.format(fileCounter)
                    data['chunkData'][fileCounter]['BytePositionStart'] = bytePositionStart
                    data['chunkData'][fileCounter]['BytePositionEnd']   = f.tell()
                    data['chunkData'][fileCounter]['chunkSize']         = sys.getsizeof(chunk)
                    data['chunkData'][fileCounter]['chunkMD5Hash']      = file_hash.hexdigest()

                    chunkFileName = data['chunkData'][fileCounter]['position'] \
                        + '_' + data['chunkData'][fileCounter]['chunkMD5Hash'] + '.part'

                    of = open(outputDir + '/' + chunkFileName, "wb")
                    of.write(chunk)
                    of.close
                    bytePositionEnd = f.tell()
                    if verbose == True:
                        posInfo = "Start Position: " + str(bytePositionStart) + " / End Position: " + str(bytePositionEnd)
                        print("Writing part file: " + chunkFileName + " (" + posInfo + ")")
                    fileCounter+=1
    except:
        print("There was an issue processing the file. Quitting!", file=sys.stderr)
        raise

    if blockString is None:
        data['numChunks'] = len(data['chunkData'])
        data['fileMD5Hash'] = full_file_hash.hexdigest()
        jsonData = json.dumps(data, sort_keys=True, indent=4)

        manifestFileOutput.write(jsonData)
        manifestFileOutput.close

        if verbose == True:
            print("Processed " + inputFile + " into " + str(data['numChunks']) + " files")
            print("Wrote manifest file and output files to " + outputDir)

if __name__ == "__main__":
    main()
