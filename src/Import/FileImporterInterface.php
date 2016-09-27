<?php

namespace Graze\DataDb\Import;

use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Node\FileNodeInterface;

interface FileImporterInterface
{
    /**
     * @param FileNodeInterface $file
     *
     * @return TableNodeInterface
     */
    public function import(FileNodeInterface $file);
}
