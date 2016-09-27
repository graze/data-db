<?php

namespace Graze\DataDb;

use Graze\DataDb\Adapter\AdapterInterface;

class TableNode implements TableNodeInterface
{
    use SoftColumnTrait;

    /**
     * @var AdapterInterface
     */
    private $adapter;

    /**
     * @var string
     */
    private $schema;

    /**
     * @var string
     */
    private $table;

    /**
     * @var string
     */
    private $columns;

    /**
     * TableNode constructor.
     *
     * @param AdapterInterface $adapter
     * @param string           $schema
     * @param string           $table
     */
    public function __construct(AdapterInterface $adapter, $schema, $table)
    {
        $this->adapter = $adapter;
        $this->schema = $schema;
        $this->table = $table;
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return $this->getFullName();
    }

    /**
     * Return a clone of this object
     *
     * @return static
     */
    public function getClone()
    {
        return clone $this;
    }

    /**
     * @return AdapterInterface
     */
    public function getAdapter()
    {
        return $this->adapter;
    }

    /**
     * @param AdapterInterface $adapter
     *
     * @return static
     */
    public function setAdapter(AdapterInterface $adapter)
    {
        $this->adapter = $adapter;
        return $this;
    }

    /**
     * @return string
     */
    public function getSchema()
    {
        return $this->schema;
    }

    /**
     * @param string $schema
     *
     * @return static
     */
    public function setSchema($schema)
    {
        $this->schema = $schema;

        return $this;
    }

    /**
     * @return string
     */
    public function getTable()
    {
        return $this->table;
    }

    /**
     * @param string $table
     *
     * @return static
     */
    public function setTable($table)
    {
        $this->table = $table;

        return $this;
    }

    /**
     * @return string
     */
    public function getFullName()
    {
        return sprintf('%s.%s', $this->getSchema(), $this->getTable());
    }

    /**
     * @return string[]
     */
    public function getColumns()
    {
        return $this->columns;
    }

    /**
     * @param string[] $columns
     *
     * @return static
     */
    public function setColumns(array $columns)
    {
        $this->columns = $columns;

        return $this;
    }
}
