<?php

namespace Graze\DataDb\Helper;

use InvalidArgumentException;
use IteratorIterator;
use Traversable;

class ChunkedIterator extends IteratorIterator
{
    /** @var int Size of each chunk */
    protected $chunkSize;

    /** @var array Current chunk */
    protected $chunk;

    /** @var int */
    protected $key;

    /**
     * @param Traversable $iterator  Traversable iterator
     * @param int         $chunkSize Size to make each chunk
     */
    public function __construct(Traversable $iterator, $chunkSize)
    {
        $chunkSize = (int) $chunkSize;
        if ($chunkSize < 1) {
            throw new InvalidArgumentException("The chunk size must be equal or greater than zero; $chunkSize given");
        }
        parent::__construct($iterator);
        $this->chunkSize = $chunkSize;
    }

    public function rewind()
    {
        parent::rewind();
        $this->key = -1;
        $this->next();
    }

    public function key()
    {
        return $this->key;
    }

    public function next()
    {
        $this->chunk = array();
        for ($i = 0; $i < $this->chunkSize && parent::valid(); $i++) {
            $this->chunk[] = parent::current();
            parent::next();
        }
        $this->key++;
    }

    /**
     * @return array
     */
    public function current()
    {
        return $this->chunk;
    }

    /**
     * @return bool
     */
    public function valid()
    {
        return (bool) $this->chunk;
    }
}
