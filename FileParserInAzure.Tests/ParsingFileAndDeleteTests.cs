using System;
using System.IO;
using AzureFunctionDurableSubscriber;
using Xunit;

namespace FileParserInAzure.Tests
{
    public class ParsingFileAndDeleteTests
    {
        [Fact]
        public void ValidatePattern_EmptyInput_ReturnsTrue()
        {
            bool result = ParsingFileAndDelete.ValidatePattern(new StringReader(string.Empty), "anything");

            Assert.True(result);
        }

        [Fact]
        public void ValidatePattern_WildcardPattern_MatchesAllLines()
        {
            bool result = ParsingFileAndDelete.ValidatePattern(
                new StringReader("value-1\r\nvalue-2"),
                "value-*");

            Assert.True(result);
        }

        [Fact]
        public void ValidatePattern_InvalidPattern_IsTreatedAsLiteralText()
        {
            bool result = ParsingFileAndDelete.ValidatePattern(
                new StringReader("[literal"),
                "[literal");

            Assert.True(result);
        }

        [Fact]
        public void ValidatePattern_NonMatchingLine_ReturnsFalse()
        {
            bool result = ParsingFileAndDelete.ValidatePattern(
                new StringReader("value-1\r\nother"),
                "value-*");

            Assert.False(result);
        }

        [Fact]
        public void WildCardToRegular_ConvertsWildcardCharacters()
        {
            string result = ParsingFileAndDelete.WildCardToRegular("value-?");

            Assert.Equal("^value-.$", result);
        }
    }
}
