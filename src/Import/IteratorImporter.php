<?php

namespace Graze\DataDb\Import;

use Graze\DataDb\Dialect\DialectInterface;
use Graze\DataDb\Helper\ChunkedIterator;
use Graze\DataDb\TableNodeInterface;
use Graze\DataFile\Helper\Builder\BuilderAwareInterface;
use Graze\DataFile\Helper\Builder\BuilderTrait;
use Traversable;

class IteratorImporter implements IteratorImporterInterface, BuilderAwareInterface
{
    use BuilderTrait;

    /** @var TableNodeInterface */
    private $table;
    /** @var int */
    private $batchSize;
    /** @var DialectInterface */
    private $dialect;

    /**
     * IteratorImporter constructor.
     *
     * @param TableNodeInterface $table
     * @param int                $batchSize
     */
    public function __construct(TableNodeInterface $table, $batchSize = 100)
    {
        $this->table = $table;
        $this->batchSize = $batchSize;
        $this->dialect = $table->getAdapter()->getDialect();
    }

    /**
     * @param Traversable $iterator
     *
     * @return TableNodeInterface
     */
    public function import(Traversable $iterator)
    {
        $chunkIterator = $this->getBuilder()->build(ChunkedIterator::class, $iterator, $this->batchSize);
        foreach ($chunkIterator as $rows) {
            list ($sql, $bind) = $this->dialect->getInsertSyntax($this->table, $rows);
            $this->table->getAdapter()->query($sql, $bind);
        }
        return $this->table;
    }
}
