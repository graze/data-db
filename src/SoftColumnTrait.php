<?php

namespace Graze\DataDb;

trait SoftColumnTrait
{
    /** @var string|null */
    protected $added = null;
    /** @var string|null */
    protected $updated = null;
    /** @var string|null */
    protected $deleted = null;

    /**
     * @return string|null
     */
    public function getSoftAdded()
    {
        return $this->added;
    }

    /**
     * @return string|null
     */
    public function getSoftUpdated()
    {
        return $this->updated;
    }

    /**
     * @return string|null
     */
    public function getSoftDeleted()
    {
        return $this->deleted;
    }

    /**
     * @param null|string $added
     *
     * @return static
     */
    public function setSoftAdded($added)
    {
        $this->added = $added;
        return $this;
    }

    /**
     * @param null|string $updated
     *
     * @return static
     */
    public function setSoftUpdated($updated)
    {
        $this->updated = $updated;
        return $this;
    }

    /**
     * @param null|string $deleted
     *
     * @return static
     */
    public function setSoftDeleted($deleted)
    {
        $this->deleted = $deleted;
        return $this;
    }
}
