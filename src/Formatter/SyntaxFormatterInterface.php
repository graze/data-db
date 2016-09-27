<?php

namespace Graze\DataDb\Formatter;

interface SyntaxFormatterInterface
{
    /**
     * @param string $char
     *
     * @return static
     */
    public function setIdentifierQuote($char);

    /**
     * @param string $syntax
     * @param array  $params
     *
     * @return string
     */
    public function format($syntax, array $params = []);
}
