<?php

namespace Graze\DataDb\Formatter;

use BadMethodCallException;
use InvalidArgumentException;

class SyntaxFormatter implements SyntaxFormatterInterface
{
    const DEFAULT_QUOTE_CHAR = '"';

    /**
     * @var string
     */
    protected $identifierQuote = self::DEFAULT_QUOTE_CHAR;

    /**
     * @param string $char
     *
     * @return static
     */
    public function setIdentifierQuote($char)
    {
        $this->identifierQuote = $char;
        return $this;
    }

    /**
     * @param string $identifier
     *
     * @return string
     */
    protected function quoteIdentifier($identifier)
    {
        return sprintf(
            '%s%s%s',
            $this->identifierQuote,
            str_replace($this->identifierQuote, $this->identifierQuote . $this->identifierQuote, $identifier),
            $this->identifierQuote
        );
    }

    /**
     * @param string $syntax
     * @param array  $params
     *
     * @return mixed
     */
    public function format($syntax, array $params = [])
    {
        return preg_replace_callback('/(?<!\{)\{(\w+)(?:\:(\w+))?(\|q)?\}(?!\})/i', function ($match) use ($params) {
            $key = $match[1];
            $result = $match[0];
            if (isset($params[$key])) {
                if (is_object($params[$key])) {
                    if (isset($match[2])) {
                        $result = $this->returnMethodCall($params[$key], $match[2]);
                    } else {
                        throw new BadMethodCallException("No method supplied in syntax to call for object: {$key}");
                    }
                } elseif (is_array($params[$key])) {
                    if (isset($match[2])) {
                        $result = $this->returnArrayKey($params[$key], $match[2]);
                    } else {
                        throw new InvalidArgumentException("No key supplied in syntax to call for array: {$key}");
                    }
                } elseif (is_scalar($params[$key])) {
                    $result = $params[$key];
                }
            }
            $doQuote = (isset($match[3]) && ($match[3] === '|q'));
            if ($doQuote) {
                return $this->quoteIdentifier($result);
            } else {
                return $result;
            }
        }, $syntax);
    }

    /**
     * @param object $object
     * @param string $method
     *
     * @return string
     */
    private function returnMethodCall($object, $method)
    {
        $method = 'get' . ucfirst($method);
        $result = call_user_func([$object, $method]);
        if ($result) {
            return $result;
        } else {
            throw new BadMethodCallException("The result of {$method} on object is not valid");
        }
    }

    /**
     * @param array  $array
     * @param string $key
     *
     * @return string mixed
     */
    private function returnArrayKey(array $array, $key)
    {
        if (!array_key_exists($key, $array)) {
            throw new InvalidArgumentException("Missing $key for array");
        }

        $result = $array[$key];
        if ($result) {
            return $result;
        } else {
            throw new InvalidArgumentException("The returned value for $key on array is not valid");
        }
    }
}
