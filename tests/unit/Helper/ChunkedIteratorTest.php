<?php

namespace Graze\DataDb\Test\Unit\Helper;

use ArrayIterator;
use Graze\DataDb\Helper\ChunkedIterator;
use Graze\DataDb\Test\TestCase;
use InvalidArgumentException;
use IteratorIterator;

class ChunkedIteratorTest extends TestCase
{
    public function testInvalidChunkSizeWillThrowAnException()
    {
        $base = new ArrayIterator(['first', 'second']);

        static::expectException(InvalidArgumentException::class);

        new ChunkedIterator($base, 0);
    }

    public function testInstanceOf()
    {
        $base = new ArrayIterator(['first', 'second']);
        $iterator = new ChunkedIterator($base, 1);
        static::assertInstanceOf(IteratorIterator::class, $iterator);
    }

    public function testSingleChunking()
    {
        $base = new ArrayIterator(['first', 'second']);
        $iterator = new ChunkedIterator($base, 1);
        static::assertEquals(
            [
                ['first'],
                ['second'],
            ],
            iterator_to_array($iterator)
        );
    }

    public function testMultipleChunking()
    {
        $base = new ArrayIterator(['first', 'second', 'third', 'fourth']);
        $iterator = new ChunkedIterator($base, 2);
        static::assertEquals(
            [
                ['first', 'second'],
                ['third', 'fourth'],
            ],
            iterator_to_array($iterator)
        );
    }

    public function testMultipleIterationsThroughTheIterator()
    {
        $base = new ArrayIterator(['first', 'second', 'third', 'fourth']);
        $iterator = new ChunkedIterator($base, 2);
        static::assertEquals(
            [
                ['first', 'second'],
                ['third', 'fourth'],
            ],
            iterator_to_array($iterator)
        );
        static::assertEquals(
            [
                ['first', 'second'],
                ['third', 'fourth'],
            ],
            iterator_to_array($iterator)
        );
    }

    public function testIteratorMethods()
    {
        $base = new ArrayIterator(['first', 'second', 'third', 'fourth']);
        $iterator = new ChunkedIterator($base, 2);
        $iterator->rewind();
        static::assertTrue($iterator->valid());
        static::assertEquals(['first', 'second'], $iterator->current());
        $iterator->next();
        static::assertTrue($iterator->valid());
        static::assertEquals(['third', 'fourth'], $iterator->current());
        $iterator->next();
        static::assertFalse($iterator->valid());
    }
}
