<?php

namespace Graze\DataDb;

use Graze\DataDb\Adapter\AdapterInterface;
use Traversable;

class QueryNode implements QueryNodeInterface
{
    /**
     * @var AdapterInterface
     */
    private $adapter;

    /**
     * @ string
     */
    private $sql;

    /**
     * @var array
     */
    private $bind = [];

    /**
     * @var string[]
     */
    private $columns = [];

    /**
     * QueryNode constructor.
     *
     * @param AdapterInterface $adapter
     * @param string           $sql
     * @param array            $bind
     */
    public function __construct(AdapterInterface $adapter, $sql, array $bind = [])
    {
        $this->adapter = $adapter;
        $this->sql = $sql;
        $this->bind = $bind;
    }

    #region Properties

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
     * @return mixed
     */
    public function getSql()
    {
        return $this->sql;
    }

    /**
     * @param mixed $sql
     *
     * @return static
     */
    public function setSql($sql)
    {
        $this->sql = $sql;
        return $this;
    }

    /**
     * @return array
     */
    public function getBind()
    {
        return $this->bind;
    }

    /**
     * @param array $bind
     *
     * @return static
     */
    public function setBind(array $bind)
    {
        $this->bind = $bind;
        return $this;
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

    #endregion

    #region Query

    /**
     * @return mixed
     */
    public function query()
    {
        return $this->adapter->query($this->sql, $this->bind);
    }

    /**
     * @return Traversable
     */
    public function fetch()
    {
        return $this->adapter->fetch($this->sql, $this->bind);
    }

    /**
     * @return array
     */
    public function fetchAll()
    {
        return $this->adapter->fetchAll($this->sql, $this->bind);
    }

    /**
     * @return array
     */
    public function fetchRow()
    {
        return $this->adapter->fetchRow($this->sql, $this->bind);
    }

    /**
     * @return mixed
     */
    public function fetchOne()
    {
        return $this->adapter->fetchOne($this->sql, $this->bind);
    }

    #endregion

    /**
     * @return string
     */
    public function __toString()
    {
        return 'Query: ' . substr($this->sql, 0, 18) . '...';
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
}
