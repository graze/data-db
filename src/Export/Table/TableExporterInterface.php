<?php

namespace Graze\DataDb\Export\Table;

use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Node\FileNodeInterface;

interface TableExporterInterface
{
    /**
     * Export a table to something
     *
     * @param TableNodeInterface $table
     *
     * @return FileNodeInterface
     */
    public function export(TableNodeInterface $table);
}
