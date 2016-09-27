<?php

namespace Graze\DataDb\Test\Unit\Formatter;

use BadMethodCallException;
use Graze\DataDb\Formatter\SyntaxFormatter;
use Graze\DataDb\Formatter\SyntaxFormatterInterface;
use Graze\DataDb\Test\TestCase;
use InvalidArgumentException;
use Mockery as m;

class SyntaxFormatterTest extends TestCase
{
    /**
     * @var SyntaxFormatter
     */
    private $formatter;

    public function setUp()
    {
        $this->formatter = new SyntaxFormatter();
    }

    public function testInstanceOf()
    {
        static::assertInstanceOf(SyntaxFormatterInterface::class, $this->formatter);
    }

    public function testUnmatchedBracketsReturnTheInput()
    {
        $syntax = 'select {stuff} from {table}';

        static::assertEquals($syntax, $this->formatter->format($syntax));
    }

    public function testSimpleReplacementMatch()
    {
        static::assertEquals(
            'select * from table',
            $this->formatter->format(
                'select {stuff} from {table}',
                [
                    'stuff' => '*',
                    'table' => 'table',
                ]
            )
        );
    }

    public function testExtraBracketsAreIgnored()
    {
        static::assertEquals(
            'select {{stuff} from {stuff}} where cake',
            $this->formatter->format(
                'select {{stuff} from {stuff}} where {stuff}',
                ['stuff' => 'cake']
            )
        );
    }

    public function testMethodRetrieval()
    {
        $mock = m::mock();
        $mock->shouldReceive('getName')
             ->andReturn('foo');

        static::assertEquals(
            'select * from foo',
            $this->formatter->format(
                'select * from {table:name}',
                ['table' => $mock]
            )
        );
    }

    public function testArrayKeyRetrieval()
    {
        $foo = ['name' => 'value'];

        static::assertEquals(
            'select * from value',
            $this->formatter->format(
                'select * from {foo:name}',
                ['foo' => $foo]
            )
        );
    }

    public function testQuotingOnLiteral()
    {
        static::assertEquals(
            'select * from "table"',
            $this->formatter->format(
                'select * from {name|q}',
                ['name' => 'table']
            )
        );
    }

    public function testQuotingOnObject()
    {
        $mock = m::mock();
        $mock->shouldReceive('getName')
             ->andReturn('foo');

        static::assertEquals(
            'select * from "foo"',
            $this->formatter->format(
                'select * from {mock:name|q}',
                ['mock' => $mock]
            )
        );
    }

    public function testQuotingOnArray()
    {
        static::assertEquals(
            'select * from "bar"',
            $this->formatter->format(
                'select * from {foo:name|q}',
                ['foo' => ['name' => 'bar']]
            )
        );
    }

    public function testCustomQuotes()
    {
        $this->formatter->setIdentifierQuote('`');

        static::assertEquals(
            'select `col``name` from `table`',
            $this->formatter->format(
                'select {col|q} from {table|q}',
                ['col' => 'col`name', 'table' => 'table']
            )
        );
    }

    public function testObjectFieldWithNoMethodWillThrowAnException()
    {
        $mock = m::mock();

        static::expectException(BadMethodCallException::class);
        $this->formatter->format('select {obj}', ['obj' => $mock]);
    }

    public function testArrayFieldWithNoKeyWillThrowAnException()
    {
        static::expectException(InvalidArgumentException::class);
        $this->formatter->format('select {arr}', ['arr' => []]);
    }

    public function testObjectWithNoMatchingMethodWillThrowAnException()
    {
        $mock = m::mock();

        static::expectException(BadMethodCallException::class);
        $this->formatter->format('select {obj:name}', ['obj' => $mock]);
    }

    public function testArrayWithNoKeyWillThrowAnException()
    {
        static::expectException(InvalidArgumentException::class);
        $this->formatter->format('select {arr:name}', ['arr' => []]);
    }

    public function testObjectReturnsNullWillThrowAnException()
    {
        $mock = m::mock();
        $mock->shouldReceive('getName')
             ->andReturnNull();

        static::expectException(BadMethodCallException::class);
        $this->formatter->format('select {obj:name}', ['obj' => $mock]);
    }

    public function testArrayReturnsNullWillThrowAnException()
    {
        static::expectException(InvalidArgumentException::class);
        $this->formatter->format('select {arr:name}', ['arr' => ['name' => null]]);
    }
}
