<?php

namespace Graze\DataDb\Import;

use Graze\DataDb\TableNodeInterface;
use Traversable;

interface IteratorImporterInterface
{
    /**
     * @param Traversable $iterator
     *
     * @return TableNodeInterface
     */
    public function import(Traversable $iterator);
}
