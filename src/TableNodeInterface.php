<?php

namespace Graze\DataDb;

use Graze\DataDb\Adapter\AdapterInterface;
use Graze\DataNode\NodeInterface;

interface TableNodeInterface extends NodeInterface
{
    /**
     * @return AdapterInterface
     */
    public function getAdapter();

    /**
     * @param AdapterInterface $adapter
     *
     * @return static
     */
    public function setAdapter(AdapterInterface $adapter);

    /**
     * @return string
     */
    public function getSchema();

    /**
     * @param string $schema
     *
     * @return static
     */
    public function setSchema($schema);

    /**
     * @return string
     */
    public function getTable();

    /**
     * @param string $table
     *
     * @return static
     */
    public function setTable($table);

    /**
     * @return string
     */
    public function getFullName();

    /**
     * @return string[]
     */
    public function getColumns();

    /**
     * @param string[] $columns
     *
     * @return static
     */
    public function setColumns(array $columns);

    /**
     * @return string
     */
    public function getSoftAdded();

    /**
     * @return string
     */
    public function getSoftUpdated();

    /**
     * @return string
     */
    public function getSoftDeleted();

    /**
     * @param string|null $added
     *
     * @return static
     */
    public function setSoftAdded($added);

    /**
     * @param string|null $updated
     *
     * @return static
     */
    public function setSoftUpdated($updated);

    /**
     * @param string|null $deleted
     *
     * @return static
     */
    public function setSoftDeleted($deleted);
}
