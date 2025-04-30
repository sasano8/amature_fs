class RFC7807Error(Exception):
    def __init__(
        self,
        title: str,
        status: int = 500,
        type: str = "about:blank",
        detail=None,
        instance=None,
        extensions=None,
    ):
        _detail = detail or ""
        super().__init__(title + ": " + _detail)
        self.type = type
        self.title = title
        self.status = status
        self.detail = detail
        self.instance = instance
        self.extensions = extensions

    def to_dict(self):
        """RFC7807形式のdictを返す"""
        error = {
            "status": self.status,
            "title": self.title,
            "type": self.type,
        }
        if self.detail is not None:
            if not (self.status >= 500):
                error["detail"] = self.detail

        if self.instance is not None:
            error["instance"] = self.instance

        if self.extensions is not None:
            error["extensions"] = self.extensions

        return error

    # title は エラー種別毎に固定されるべき。エラー種別は type: https://example.com/probs/resource-locked などの単位。
    # status_code の粒度とは少し違うけれど、その単位に合わせておく方がスタートアップでは現実的。
    @classmethod
    def internalservererror(cls, *args, **kwargs):
        return cls(
            *args,
            type="https://example.com/probs/internalservererror",
            title="Internal Server Error.",
            status=500,
            **kwargs,
        )

    @classmethod
    def not_catalog(cls, *args, **kwargs):
        """カタログとして想定するディレクトリ構造やファイル構造でない場合に生じるエラー"""
        return cls(
            *args,
            type="https://example.com/probs/not_catalog",
            title="Not Catalog Error.",
            status=500,
            **kwargs,
        )

    @classmethod
    def load_catalog_json_error(cls, *args, **kwargs):
        return cls(
            *args,
            type="https://example.com/probs/load_catalog_json_error",
            title="Can't load catalog.json.",
            status=500,
            **kwargs,
        )

    @classmethod
    def dump_catalog_json_error(cls, *args, **kwargs):
        return cls(
            *args,
            type="https://example.com/probs/dump_catalog_json_error",
            title="Can't dump catalog.json.",
            status=500,
            **kwargs,
        )

    @classmethod
    def catalog_json_already_exists_error(cls, *args, **kwargs):
        return cls(
            *args,
            type="https://example.com/probs/catalog_json__already_exists_error",
            title="Already exists catalog.json.",
            status=500,
            **kwargs,
        )

    @classmethod
    def jsondecodeerror(cls, *args, **kwargs):
        return cls(
            *args,
            type="https://example.com/probs/jsondecodeerror",
            title="Json decode error.",
            status=500,
            **kwargs,
        )

    @classmethod
    def unauthorized(cls, *args, **kwargs):
        return cls(
            *args,
            type="https://example.com/probs/unauthorized",
            title="Unauthorized.",
            status=401,
            **kwargs,
        )

    @classmethod
    def forbidden(cls, *args, **kwargs):
        return cls(
            *args,
            type="https://example.com/probs/resource-forbidden",
            title="Resource forbidden.",
            status=403,
            **kwargs,
        )

    @classmethod
    def not_found(cls, *args, **kwargs):
        return cls(
            *args,
            type="https://example.com/probs/resource-notfound",
            title="Resource Not Found.",
            status=404,
            **kwargs,
        )

    @classmethod
    def resource_locked(cls, *args, **kwargs):
        return cls(
            *args,
            type="https://example.com/probs/resource-locked",
            title="Resource Locked.",
            status=409,
            **kwargs,
        )

    @classmethod
    def unprocessableEntity(cls, *args, **kwargs):
        return cls(
            *args,
            type="https://example.com/probs/unprocessableEntity",
            title="UnprocessableEntity.",
            status=422,
            **kwargs,
        )

    @classmethod
    def serializeerror(cls, *args, **kwargs):
        return cls(
            *args,
            type="https://example.com/probs/serializeerror",
            title="Serialize Error.",
            status=422,
            **kwargs,
        )

    @classmethod
    def file_integrity_error(cls, *args, **kwargs):
        return cls(
            *args,
            type="https://example.com/probs/file_integrity_error",
            title="File Integrity Error.",
            status=500,
            **kwargs,
        )
