<?php

namespace Graze\DataDb\Export\Query;

use Graze\DataDb\QueryNodeInterface;
use Graze\DataFile\Node\FileNodeInterface;

interface QueryExporterInterface
{
    /**
     * Export a table to something
     *
     * @param QueryNodeInterface $query
     *
     * @return FileNodeInterface
     */
    public function export(QueryNodeInterface $query);
}
