// when you get past the many pages of java-beauracracy
// you eventually get to the bit where something actually
// happens and it doesn't take more LOC than any other language.

public void writeInt(int value) throws IOException {
  if (value == previousValue) {
    // keep track of how many times we've seen this value
    // consecutively
    ++repeatCount;

    if (repeatCount >= 8) {
      // we've seen this at least 8 times, we're
      // certainly going to write an rle-run,
      // so just keep on counting repeats for now
      return;
    }
  } else {
    // This is a new value, check if it signals the end of
    // an rle-run
    if (repeatCount >= 8) {
      // it does! write an rle-run
      writeRleRun();
    }

    // this is a new value so we've only seen it once
    repeatCount = 1;
    // start tracking this value for repeats
    previousValue = value;
  }

  // We have not seen enough repeats to justify an rle-run yet,
  // so buffer this value in case we decide to write a bit-packed-run
  bufferedValues[numBufferedValues] = value;
  ++numBufferedValues;

  if (numBufferedValues == 8) {
    // we've encountered less than 8 repeated values, so
    // either start a new bit-packed-run or append to the
    // current bit-packed-run
    writeOrAppendBitPackedRun();
  }
}

private void writeOrAppendBitPackedRun() throws IOException {
  if (bitPackedGroupCount >= 63) {
    // we've packed as many values as we can for this run,
    // end it and start a new one
    endPreviousBitPackedRun();
  }

  if (bitPackedRunHeaderPointer == -1) {
    // this is a new bit-packed-run, allocate a byte for the header
    // and keep a "pointer" to it so that it can be mutated later
    baos.write(0); // write a sentinel value
    bitPackedRunHeaderPointer = baos.getCurrentIndex();
  }

  packer.pack8Values(bufferedValues, 0, packBuffer, 0);
  baos.write(packBuffer);

  // empty the buffer, they've all been written
  numBufferedValues = 0;

  // clear the repeat count, as some repeated values
  // may have just been bit packed into this run
  repeatCount = 0;

  ++bitPackedGroupCount;
}

/**
 * If we are currently writing a bit-packed-run, update the
 * bit-packed-header and consider this run to be over
 *
 * does nothing if we're not currently writing a bit-packed run
 */
private void endPreviousBitPackedRun() {
  if (bitPackedRunHeaderPointer == -1) {
    // we're not currently in a bit-packed-run
    return;
  }

  // create bit-packed-header, which needs to fit in 1 byte
  byte bitPackHeader = (byte) ((bitPackedGroupCount << 1) | 1);

  // update this byte
  baos.setByte(bitPackedRunHeaderPointer, bitPackHeader);

  // mark that this run is over
  bitPackedRunHeaderPointer = -1;

  // reset the number of groups
  bitPackedGroupCount = 0;
}

private void writeRleRun() throws IOException {
  // we may have been working on a bit-packed-run
  // so close that run if it exists before writing this
  // rle-run
  endPreviousBitPackedRun();

  // write the rle-header (lsb of 0 signifies a rle run)
  BytesUtils.writeUnsignedVarInt(repeatCount << 1, baos);
  // write the repeated-value
  BytesUtils.writeIntLittleEndianPaddedOnBitWidth(baos, previousValue, bitWidth);

  // reset the repeat count
  repeatCount = 0;

  // throw away all the buffered values, they were just repeats and they've been written
  numBufferedValues = 0;
}




