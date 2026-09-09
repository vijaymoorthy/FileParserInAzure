using System;
using System.IO;
using System.Linq;
using System.Threading;
using Fileprocessing;
using Xunit;

namespace FileParserInAzure.Tests
{
    public class FilespliterTests
    {
        [Fact]
        public void SplitFile_EmptyInput_ReturnsEmptyChunk()
        {
            var result = SendData.SplitFile(new StringReader(string.Empty), "file.txt", 2, CancellationToken.None);

            Assert.Single(result);
            Assert.Equal("file.txt_2", result[0].FileName);
            Assert.Equal(string.Empty, result[0].TextLine);
        }

        [Fact]
        public void SplitFile_ZeroMaxLineSize_ThrowsArgumentOutOfRangeException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => SendData.SplitFile(
                new StringReader("line"),
                "file.txt",
                0,
                CancellationToken.None));
        }

        [Fact]
        public void SplitFile_ExactBoundary_AddsTrailingEmptyChunkUsingCurrentContract()
        {
            var result = SendData.SplitFile(new StringReader("one\r\ntwo"), "file.txt", 2, CancellationToken.None);

            Assert.Equal(2, result.Count);
            Assert.Equal("file.txt_1", result[0].FileName);
            Assert.Equal("one\r\ntwo\r\n", result[0].TextLine);
            Assert.Equal("file.txt_3", result[1].FileName);
            Assert.Equal(string.Empty, result[1].TextLine);
        }

        [Fact]
        public void SplitFile_Remainder_CreatesNumberedChunks()
        {
            var result = SendData.SplitFile(new StringReader("one\r\ntwo\r\nthree"), "file.txt", 2, CancellationToken.None);

            Assert.Equal(new[] { "file.txt_1", "file.txt_3" }, result.Select(item => item.FileName));
            Assert.Equal("three\r\n", result[1].TextLine);
        }

        [Fact]
        public void SplitFile_CancelledToken_ThrowsOperationCanceledException()
        {
            Assert.Throws<OperationCanceledException>(() => SendData.SplitFile(
                new StringReader("line"),
                "file.txt",
                1,
                new CancellationToken(true)));
        }
    }
}
